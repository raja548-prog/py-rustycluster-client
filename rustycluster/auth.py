"""
Authentication manager for RustyCluster.

Handles initial authentication and token storage. The session token is
injected into gRPC calls via AuthInterceptor (see interceptors.py).
"""

from __future__ import annotations

import logging
import threading

import grpc

from rustycluster.exceptions import AuthenticationError

logger = logging.getLogger("rustycluster.auth")


class AuthManager:
    """
    Manages session token lifecycle for a RustyCluster gRPC connection.

    Thread-safe: uses a lock to guard token reads/writes.

    Args:
        stub: The gRPC KeyValueServiceStub to call Authenticate on.
        username: Authentication username.
        password: Authentication password.
    """

    def __init__(self, stub: Any, username: str, password: str) -> None:  # type: ignore[name-defined]
        self._stub = stub
        self._username = username
        self._password = password
        self._token: str = ""
        self._lock = threading.RLock()

    def authenticate(self, timeout: float = 10.0) -> str:
        """
        Perform authentication and store the session token.

        Args:
            timeout: RPC timeout in seconds.

        Returns:
            The session token string.

        Raises:
            AuthenticationError: If credentials are rejected.
        """
        from rustycluster.proto import rustycluster_pb2

        logger.debug("Authenticating user '%s'", self._username)
        try:
            response = self._stub.Authenticate(
                rustycluster_pb2.AuthenticateRequest(
                    username=self._username,
                    creds=self._password,
                ),
                timeout=timeout,
            )
        except grpc.RpcError as exc:
            raise AuthenticationError(
                f"Authentication RPC failed: {exc.details() if hasattr(exc, 'details') else exc}",
                cause=exc,
            ) from exc

        if not response.success:
            raise AuthenticationError(
                f"Authentication rejected: {response.message or 'invalid credentials'}"
            )

        with self._lock:
            self._token = response.session_token

        logger.debug("Authentication successful for user '%s'", self._username)
        return self._token

    def get_token(self) -> str:
        """Return the current session token (thread-safe)."""
        with self._lock:
            return self._token

    def invalidate(self) -> None:
        """Clear the stored session token (forces re-auth on next call)."""
        with self._lock:
            self._token = ""
        logger.debug("Session token invalidated")

    @property
    def is_authenticated(self) -> bool:
        """Return True if a session token is currently stored."""
        with self._lock:
            return bool(self._token)


# Avoid circular import with type hint
from typing import Any  # noqa: E402
