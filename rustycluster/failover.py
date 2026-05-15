"""
Multi-node failover support for RustyCluster.

`NodeManager` owns the active gRPC channel/stub/auth-manager for the
currently selected node. On `failover()` it advances to the next node
in priority order (as configured in `RustyClusterConfig.nodes`),
rebuilds the channel, and re-authenticates against the new server.

Failure semantics: once the manager moves to a secondary, it stays
there — there is no failback. A fresh primary connection requires a
new `get_client()` call.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

import grpc
import grpc.aio

from rustycluster.auth import AuthManager
from rustycluster.config import RustyClusterConfig
from rustycluster.exceptions import AuthenticationError
from rustycluster.exceptions import ConnectionError as RCConnectionError
from rustycluster.interceptors import build_interceptors
from rustycluster.proto import rustycluster_pb2, rustycluster_pb2_grpc

logger = logging.getLogger("rustycluster.failover")


class NodeManager:
    """
    Tracks the active node and performs failover to the next node on demand.

    Args:
        config: The RustyClusterConfig with the parsed `nodes` list.
        build_channel: Callable that takes (target: str, config) and returns
            a fresh gRPC channel. Injected to keep this module decoupled from
            the channel-options policy in __init__.py.
    """

    def __init__(
        self,
        config: RustyClusterConfig,
        build_channel: Callable[[str, RustyClusterConfig], grpc.Channel],
    ) -> None:
        self._config = config
        self._build_channel = build_channel
        self._targets: list[str] = config.targets
        if not self._targets:
            raise RCConnectionError("No nodes configured")
        self._index: int = 0
        self._channel: grpc.Channel | None = None
        self._stub: Any = None
        self._auth: AuthManager | None = None
        self._connect_current()

    def _connect_current(self) -> None:
        """Build channel/stub/auth for the node at the current index and authenticate."""
        target = self._targets[self._index]
        logger.debug("Connecting to node %s", target)

        raw_channel = self._build_channel(target, self._config)
        raw_stub = rustycluster_pb2_grpc.KeyValueServiceStub(raw_channel)

        auth = AuthManager(
            stub=raw_stub,
            username=self._config.username,
            password=self._config.password.get_secret_value(),
        )
        auth.authenticate(timeout=self._config.timeout_seconds)

        interceptors = build_interceptors(token_provider=auth.get_token)
        intercepted = grpc.intercept_channel(raw_channel, *interceptors)
        stub = rustycluster_pb2_grpc.KeyValueServiceStub(intercepted)
        auth._stub = stub  # so reauth uses the intercepted channel

        self._channel = intercepted
        self._stub = stub
        self._auth = auth

    @property
    def stub(self) -> Any:
        return self._stub

    @property
    def channel(self) -> grpc.Channel:
        assert self._channel is not None
        return self._channel

    @property
    def auth_manager(self) -> AuthManager:
        assert self._auth is not None
        return self._auth

    @property
    def current_target(self) -> str:
        return self._targets[self._index]

    @property
    def remaining_nodes(self) -> int:
        """Number of nodes still available, including the current one."""
        return len(self._targets) - self._index

    def failover(self) -> None:
        """
        Advance to the next reachable node and connect to it.

        Skips past nodes that themselves fail to connect or authenticate,
        rolling forward until a working node is found. Raises
        `ConnectionError` once the list is exhausted.
        """
        old_target = self.current_target
        self._close_channel()

        errors: list[str] = []
        while self._index + 1 < len(self._targets):
            self._index += 1
            new_target = self._targets[self._index]
            logger.warning("Failing over from %s to %s", old_target, new_target)
            try:
                self._connect_current()
                return
            except Exception as exc:
                logger.warning("Node %s unreachable during failover: %s", new_target, exc)
                errors.append(f"{new_target}: {exc}")
                old_target = new_target

        raise RCConnectionError(
            "All nodes exhausted: "
            + (", ".join(errors) if errors else ", ".join(self._targets))
        )

    def close(self) -> None:
        self._close_channel()

    def _close_channel(self) -> None:
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception as exc:
                logger.warning("Error closing gRPC channel: %s", exc)
            self._channel = None
            self._stub = None
            self._auth = None


class AsyncNodeManager:
    """
    Async counterpart of NodeManager.

    Owns the active grpc.aio channel/stub/session-token for the current node
    and can fail over to the next node on demand. Authentication happens at
    construction time via `connect()` (since async work cannot run in __init__).
    """

    def __init__(
        self,
        config: RustyClusterConfig,
        build_channel: Callable[[str, RustyClusterConfig], grpc.aio.Channel],
    ) -> None:
        self._config = config
        self._build_channel = build_channel
        self._targets: list[str] = config.targets
        if not self._targets:
            raise RCConnectionError("No nodes configured")
        self._index: int = 0
        self._channel: grpc.aio.Channel | None = None
        self._stub: Any = None
        self._token: str = ""

    async def connect(self) -> None:
        """Connect to the primary and authenticate. Call once after construction."""
        await self._connect_current()

    async def _connect_current(self) -> None:
        target = self._targets[self._index]
        logger.debug("Connecting to async node %s", target)

        channel = self._build_channel(target, self._config)
        stub = rustycluster_pb2_grpc.KeyValueServiceStub(channel)

        try:
            resp = await stub.Authenticate(
                rustycluster_pb2.AuthenticateRequest(
                    username=self._config.username,
                    creds=self._config.password.get_secret_value(),
                ),
                timeout=self._config.timeout_seconds,
            )
        except grpc.RpcError as exc:
            await channel.close()
            raise AuthenticationError(
                f"Async authentication failed against {target}: "
                f"{exc.details() if hasattr(exc, 'details') else exc}",
                cause=exc,
            ) from exc

        if not resp.success:
            await channel.close()
            raise AuthenticationError(f"Authentication rejected at {target}: {resp.message}")

        self._channel = channel
        self._stub = stub
        self._token = resp.session_token

    @property
    def stub(self) -> Any:
        return self._stub

    @property
    def channel(self) -> grpc.aio.Channel:
        assert self._channel is not None
        return self._channel

    @property
    def token(self) -> str:
        return self._token

    @property
    def current_target(self) -> str:
        return self._targets[self._index]

    async def reauthenticate(self) -> None:
        """Refresh the session token on the current node."""
        assert self._stub is not None
        try:
            resp = await self._stub.Authenticate(
                rustycluster_pb2.AuthenticateRequest(
                    username=self._config.username,
                    creds=self._config.password.get_secret_value(),
                ),
                timeout=self._config.timeout_seconds,
            )
        except grpc.RpcError as exc:
            raise AuthenticationError("Re-authentication failed", cause=exc) from exc

        if not resp.success:
            raise AuthenticationError(f"Authentication rejected: {resp.message}")
        self._token = resp.session_token

    async def failover(self) -> None:
        """
        Advance to the next reachable node and connect to it.

        Skips past nodes that themselves fail to connect or authenticate,
        rolling forward until a working node is found. Raises
        `ConnectionError` once the list is exhausted.
        """
        old_target = self.current_target
        await self._close_channel()

        errors: list[str] = []
        while self._index + 1 < len(self._targets):
            self._index += 1
            new_target = self._targets[self._index]
            logger.warning("Failing over from %s to %s", old_target, new_target)
            try:
                await self._connect_current()
                return
            except Exception as exc:
                logger.warning("Node %s unreachable during failover: %s", new_target, exc)
                errors.append(f"{new_target}: {exc}")
                old_target = new_target

        raise RCConnectionError(
            "All nodes exhausted: "
            + (", ".join(errors) if errors else ", ".join(self._targets))
        )

    async def close(self) -> None:
        await self._close_channel()

    async def _close_channel(self) -> None:
        if self._channel is not None:
            try:
                await self._channel.close()
            except Exception as exc:
                logger.warning("Error closing async gRPC channel: %s", exc)
            self._channel = None
            self._stub = None
            self._token = ""
