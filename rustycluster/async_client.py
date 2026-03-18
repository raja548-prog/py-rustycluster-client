"""
Async RustyCluster client using grpc.aio.

Mirrors the synchronous RustyClusterClient API with async/await methods.

Example:
    import asyncio
    from rustycluster import async_get_client, RustyClusterConfig

    async def main():
        config = RustyClusterConfig(host="localhost", port=50051,
                                     username="admin", password="secret")
        async with async_get_client(config) as client:
            await client.set("hello", "world")
            value = await client.get("hello")

    asyncio.run(main())
"""

from __future__ import annotations

import logging
from types import TracebackType
from typing import Optional

import grpc
import grpc.aio

from rustycluster.config import RustyClusterConfig
from rustycluster.exceptions import (
    AuthenticationError,
    BatchOperationError,
    from_grpc_error,
)
from rustycluster.proto import rustycluster_pb2, rustycluster_pb2_grpc

logger = logging.getLogger("rustycluster.async_client")


class AsyncRustyClusterClient:
    """
    Async Python client for RustyCluster using grpc.aio.

    Use async_get_client() to create a fully initialized instance.
    Supports async context managers.
    """

    def __init__(
        self,
        stub: rustycluster_pb2_grpc.KeyValueServiceStub,
        config: RustyClusterConfig,
        channel: grpc.aio.Channel,
        session_token: str,
    ) -> None:
        self._stub = stub
        self._config = config
        self._channel = channel
        self._token = session_token

    async def __aenter__(self) -> "AsyncRustyClusterClient":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying async gRPC channel."""
        await self._channel.close()
        logger.debug("Async gRPC channel closed")

    def _metadata(self) -> list[tuple[str, str]]:
        return [("authorization", f"Bearer {self._token}")]

    async def reauthenticate(self) -> None:
        """Re-authenticate and refresh the session token."""
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

    # ──────────────────────────────────────────────
    # System
    # ──────────────────────────────────────────────

    async def ping(self) -> bool:
        resp = await self._stub.Ping(
            rustycluster_pb2.PingRequest(),
            timeout=self._config.timeout_seconds,
            metadata=self._metadata(),
        )
        return resp.success

    async def health_check(self) -> bool:
        resp = await self._stub.HealthCheck(
            rustycluster_pb2.PingRequest(),
            timeout=self._config.timeout_seconds,
            metadata=self._metadata(),
        )
        return resp.success

    # ──────────────────────────────────────────────
    # String operations
    # ──────────────────────────────────────────────

    async def set(self, key: str, value: str, skip_replication: bool = False, skip_site_replication: bool = False) -> bool:
        resp = await self._stub.Set(
            rustycluster_pb2.SetRequest(key=key, value=value, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.success

    async def get(self, key: str) -> Optional[str]:
        resp = await self._stub.Get(
            rustycluster_pb2.GetRequest(key=key),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.value if resp.found else None

    async def delete(self, key: str, skip_replication: bool = False, skip_site_replication: bool = False) -> bool:
        resp = await self._stub.Delete(
            rustycluster_pb2.DeleteRequest(key=key, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.success

    async def set_ex(self, key: str, value: str, ttl: int, skip_replication: bool = False, skip_site_replication: bool = False) -> bool:
        resp = await self._stub.SetEx(
            rustycluster_pb2.SetExRequest(key=key, value=value, ttl=ttl, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.success

    async def set_expiry(self, key: str, ttl: int, skip_replication: bool = False, skip_site_replication: bool = False) -> bool:
        resp = await self._stub.SetExpiry(
            rustycluster_pb2.SetExpiryRequest(key=key, ttl=ttl, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.success

    async def set_nx(self, key: str, value: str, skip_replication: bool = False, skip_site_replication: bool = False) -> bool:
        resp = await self._stub.SetNX(
            rustycluster_pb2.SetNXRequest(key=key, value=value, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.success

    async def exists(self, key: str) -> bool:
        resp = await self._stub.Exists(
            rustycluster_pb2.ExistsRequest(key=key),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.exists

    # ──────────────────────────────────────────────
    # Numeric
    # ──────────────────────────────────────────────

    async def incr_by(self, key: str, value: int, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.IncrBy(
            rustycluster_pb2.IncrByRequest(key=key, value=value, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.new_value

    async def decr_by(self, key: str, value: int, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.DecrBy(
            rustycluster_pb2.DecrByRequest(key=key, value=value, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.new_value

    async def incr_by_float(self, key: str, value: float, skip_replication: bool = False, skip_site_replication: bool = False) -> float:
        resp = await self._stub.IncrByFloat(
            rustycluster_pb2.IncrByFloatRequest(key=key, value=value, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.new_value

    # ──────────────────────────────────────────────
    # Hash
    # ──────────────────────────────────────────────

    async def hset(self, key: str, field: str, value: str, skip_replication: bool = False, skip_site_replication: bool = False) -> bool:
        resp = await self._stub.HSet(
            rustycluster_pb2.HSetRequest(key=key, field=field, value=value, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.success

    async def hget(self, key: str, field: str) -> Optional[str]:
        resp = await self._stub.HGet(
            rustycluster_pb2.HGetRequest(key=key, field=field),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.value if resp.found else None

    async def hget_all(self, key: str) -> dict[str, str]:
        resp = await self._stub.HGetAll(
            rustycluster_pb2.HGetAllRequest(key=key),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return dict(resp.fields)

    async def hmset(self, key: str, fields: dict[str, str], skip_replication: bool = False, skip_site_replication: bool = False) -> bool:
        resp = await self._stub.HMSet(
            rustycluster_pb2.HMSetRequest(key=key, fields=fields, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.success

    async def hexists(self, key: str, field: str) -> bool:
        resp = await self._stub.HExists(
            rustycluster_pb2.HExistsRequest(key=key, field=field),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.exists

    async def hdel(self, key: str, *fields: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.HDel(
            rustycluster_pb2.HDelRequest(key=key, fields=list(fields), skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.deleted_count

    async def hlen(self, key: str) -> int:
        resp = await self._stub.HLen(
            rustycluster_pb2.HLenRequest(key=key),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.length

    async def sadd(self, key: str, *members: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.SAdd(
            rustycluster_pb2.SAddRequest(key=key, members=list(members), skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.added_count

    async def smembers(self, key: str) -> list[str]:
        resp = await self._stub.SMembers(
            rustycluster_pb2.SMembersRequest(key=key),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return list(resp.members)

    async def srem(self, key: str, *members: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.SRem(
            rustycluster_pb2.SRemRequest(key=key, members=list(members), skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.removed_count

    async def scard(self, key: str) -> int:
        resp = await self._stub.SCard(
            rustycluster_pb2.SCardRequest(key=key),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.cardinality

    async def del_multiple(self, *keys: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.DelMultiple(
            rustycluster_pb2.DelMultipleRequest(keys=list(keys), skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.deleted_count

    async def keys(self, pattern: str) -> list[str]:
        resp = await self._stub.Keys(
            rustycluster_pb2.KeysRequest(pattern=pattern),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return list(resp.keys)

    async def mget(self, *keys: str) -> list[Optional[str]]:
        resp = await self._stub.MGet(
            rustycluster_pb2.MGetRequest(keys=list(keys)),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return [v if v != "" else None for v in resp.values]

    async def batch_write(
        self,
        operations: list[rustycluster_pb2.BatchOperation],
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> list[bool]:
        resp = await self._stub.BatchWrite(
            rustycluster_pb2.BatchWriteRequest(
                operations=operations,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
            metadata=self._metadata(),
        )
        if not resp.success:
            raise BatchOperationError("Batch write failed", results=list(resp.operation_results))
        return list(resp.operation_results)
