"""
Async RustyCluster client using grpc.aio.

Mirrors the synchronous RustyClusterClient API with async/await methods.
Supports multi-node failover via AsyncNodeManager: each call is dispatched
through `_invoke`, which catches transient errors and switches to the next
configured node before retrying.

Example:
    import asyncio
    from rustycluster import async_get_client, RustyClusterConfig

    async def main():
        config = RustyClusterConfig(
            nodes="localhost:50051,localhost:50052",
            username="admin", password="secret",
        )
        async with async_get_client(config) as client:
            await client.set("hello", "world")
            value = await client.get("hello")

    asyncio.run(main())
"""

from __future__ import annotations

import logging
from types import TracebackType
from typing import Any, Optional

import grpc
import grpc.aio

from rustycluster.config import RustyClusterConfig
from rustycluster.exceptions import (
    AuthenticationError,
    BatchOperationError,
    NON_RETRYABLE_CODES,
    REAUTH_CODES,
    from_grpc_error,
)
from rustycluster.failover import AsyncNodeManager
from rustycluster.proto import rustycluster_pb2, rustycluster_pb2_grpc

logger = logging.getLogger("rustycluster.async_client")


class _AsyncStubProxy:
    """
    Forwards `proxy.MethodName(req, ...)` to `client._invoke("MethodName", req, ...)`.

    Lets the async client keep its existing `self._stub.Foo(req, ...)` call
    style while routing every RPC through the failover-aware `_invoke` helper.
    """

    __slots__ = ("_client",)

    def __init__(self, client: "AsyncRustyClusterClient") -> None:
        self._client = client

    def __getattr__(self, name: str) -> Any:
        client = self._client

        async def call(request: Any, **kwargs: Any) -> Any:
            return await client._invoke(name, request, **kwargs)

        return call


class AsyncRustyClusterClient:
    """
    Async Python client for RustyCluster using grpc.aio.

    Use async_get_client() to create a fully initialized instance.
    Supports async context managers and multi-node failover.
    """

    def __init__(
        self,
        manager: AsyncNodeManager,
        config: RustyClusterConfig,
    ) -> None:
        self._manager = manager
        self._config = config
        self._stub = _AsyncStubProxy(self)

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
        await self._manager.close()
        logger.debug("Async gRPC channel closed")

    def _metadata(self) -> list[tuple[str, str]]:
        return [("authorization", f"Bearer {self._manager.token}")]

    async def reauthenticate(self) -> None:
        """Re-authenticate and refresh the session token on the current node."""
        await self._manager.reauthenticate()

    async def _invoke(self, method_name: str, request: Any, **kwargs: Any) -> Any:
        """
        Dispatch a single RPC, handling reauth and failover transparently.

        Always overrides the `metadata` kwarg so the freshest auth token is
        used after reauth or failover. The `timeout` kwarg defaults to the
        configured per-RPC timeout when not supplied.
        """
        if "timeout" not in kwargs:
            kwargs["timeout"] = self._config.timeout_seconds

        reauthed = False
        while True:
            kwargs["metadata"] = self._metadata()
            try:
                rpc = getattr(self._manager.stub, method_name)
                return await rpc(request, **kwargs)
            except grpc.aio.AioRpcError as exc:
                code = exc.code()
                if code in REAUTH_CODES and not reauthed:
                    try:
                        await self._manager.reauthenticate()
                        reauthed = True
                        continue
                    except Exception:
                        raise from_grpc_error(exc) from exc

                if code in NON_RETRYABLE_CODES:
                    raise from_grpc_error(exc) from exc

                # Transient (UNAVAILABLE, DEADLINE_EXCEEDED, etc.) → failover.
                try:
                    await self._manager.failover()
                except Exception:
                    raise from_grpc_error(exc) from exc
                reauthed = False  # fresh token on new node
                continue

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

    # ─── Sorted set operations ───

    async def zadd(self, key: str, score: float, member: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.ZAdd(
            rustycluster_pb2.ZAddRequest(key=key, score=score, member=member, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.added

    async def zrem(self, key: str, *members: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.ZRem(
            rustycluster_pb2.ZRemRequest(key=key, members=list(members), skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.removed

    async def zrange_by_score(self, key: str, min: float, max: float, with_scores: bool = False, offset: int = 0, count: int = 0) -> list:
        resp = await self._stub.ZRangeByScore(
            rustycluster_pb2.ZRangeByScoreRequest(key=key, min=min, max=max, has_limit=count > 0, offset=offset, count=count, with_scores=with_scores),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        if with_scores:
            return [(m.member, m.score) for m in resp.members]
        return [m.member for m in resp.members]

    async def zrange(self, key: str, start: int, stop: int, with_scores: bool = False) -> list:
        resp = await self._stub.ZRange(
            rustycluster_pb2.ZRangeRequest(key=key, start=start, stop=stop, with_scores=with_scores),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        if with_scores:
            return [(m.member, m.score) for m in resp.members]
        return [m.member for m in resp.members]

    async def zscore(self, key: str, member: str) -> Optional[float]:
        resp = await self._stub.ZScore(
            rustycluster_pb2.ZScoreRequest(key=key, member=member),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.score if resp.found else None

    async def zcard(self, key: str) -> int:
        resp = await self._stub.ZCard(
            rustycluster_pb2.ZCardRequest(key=key),
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

    # ─── List (queue) operations ───

    async def lpush(self, key: str, *values: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.LPush(
            rustycluster_pb2.LPushRequest(key=key, values=list(values), skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.length

    async def rpush(self, key: str, *values: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.RPush(
            rustycluster_pb2.RPushRequest(key=key, values=list(values), skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.length

    async def lpushx(self, key: str, *values: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.LPushX(
            rustycluster_pb2.LPushXRequest(key=key, values=list(values), skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.length

    async def rpushx(self, key: str, *values: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.RPushX(
            rustycluster_pb2.RPushXRequest(key=key, values=list(values), skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.length

    async def lpop(self, key: str, count: Optional[int] = None, skip_replication: bool = False, skip_site_replication: bool = False) -> list[str]:
        req = rustycluster_pb2.LPopRequest(key=key, skip_replication=skip_replication, skip_site_replication=skip_site_replication)
        if count is not None:
            req.count = count
        resp = await self._stub.LPop(req, timeout=self._config.timeout_seconds, metadata=self._metadata())
        return list(resp.values)

    async def rpop(self, key: str, count: Optional[int] = None, skip_replication: bool = False, skip_site_replication: bool = False) -> list[str]:
        req = rustycluster_pb2.RPopRequest(key=key, skip_replication=skip_replication, skip_site_replication=skip_site_replication)
        if count is not None:
            req.count = count
        resp = await self._stub.RPop(req, timeout=self._config.timeout_seconds, metadata=self._metadata())
        return list(resp.values)

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        resp = await self._stub.LRange(
            rustycluster_pb2.LRangeRequest(key=key, start=start, stop=stop),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return list(resp.values)

    async def llen(self, key: str) -> int:
        resp = await self._stub.LLen(
            rustycluster_pb2.LLenRequest(key=key),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.length

    async def ltrim(self, key: str, start: int, stop: int, skip_replication: bool = False, skip_site_replication: bool = False) -> bool:
        resp = await self._stub.LTrim(
            rustycluster_pb2.LTrimRequest(key=key, start=start, stop=stop, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.success

    async def lindex(self, key: str, index: int) -> Optional[str]:
        resp = await self._stub.LIndex(
            rustycluster_pb2.LIndexRequest(key=key, index=index),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.value if resp.found else None

    async def lset(self, key: str, index: int, value: str, skip_replication: bool = False, skip_site_replication: bool = False) -> bool:
        resp = await self._stub.LSet(
            rustycluster_pb2.LSetRequest(key=key, index=index, value=value, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.success

    async def lrem(self, key: str, count: int, element: str, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.LRem(
            rustycluster_pb2.LRemRequest(key=key, count=count, element=element, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.removed

    async def linsert(self, key: str, pivot: str, element: str, before: bool = True, skip_replication: bool = False, skip_site_replication: bool = False) -> int:
        resp = await self._stub.LInsert(
            rustycluster_pb2.LInsertRequest(key=key, before=before, pivot=pivot, element=element, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.length

    async def lpos(self, key: str, element: str, rank: Optional[int] = None, count: Optional[int] = None) -> list[int]:
        req = rustycluster_pb2.LPosRequest(key=key, element=element)
        if rank is not None:
            req.rank = rank
        if count is not None:
            req.count = count
        resp = await self._stub.LPos(req, timeout=self._config.timeout_seconds, metadata=self._metadata())
        return list(resp.positions)

    async def blpop(self, *keys: str, timeout: float = 0.0, skip_replication: bool = False, skip_site_replication: bool = False) -> Optional[tuple[str, str]]:
        grpc_timeout = (timeout + 5.0) if timeout > 0 else None
        resp = await self._stub.BLPop(
            rustycluster_pb2.BLPopRequest(keys=list(keys), timeout=timeout, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=grpc_timeout, metadata=self._metadata(),
        )
        return (resp.key, resp.value) if resp.key else None

    async def brpop(self, *keys: str, timeout: float = 0.0, skip_replication: bool = False, skip_site_replication: bool = False) -> Optional[tuple[str, str]]:
        grpc_timeout = (timeout + 5.0) if timeout > 0 else None
        resp = await self._stub.BRPop(
            rustycluster_pb2.BRPopRequest(keys=list(keys), timeout=timeout, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=grpc_timeout, metadata=self._metadata(),
        )
        return (resp.key, resp.value) if resp.key else None

    # ─── Stream operations ───

    async def xadd(self, key: str, fields: dict[str, str], id: str = "*", skip_replication: bool = False, skip_site_replication: bool = False) -> str:
        resp = await self._stub.XAdd(
            rustycluster_pb2.XAddRequest(key=key, id=id, fields=fields, skip_replication=skip_replication, skip_site_replication=skip_site_replication),
            timeout=self._config.timeout_seconds, metadata=self._metadata(),
        )
        return resp.id

    async def xread(self, streams: list[tuple[str, str]], count: Optional[int] = None) -> list[tuple[str, str, dict[str, str]]]:
        req = rustycluster_pb2.XReadRequest(
            streams=[rustycluster_pb2.XReadStreamKey(key=k, id=i) for k, i in streams],
        )
        if count is not None:
            req.count = count
        resp = await self._stub.XRead(req, timeout=self._config.timeout_seconds, metadata=self._metadata())
        return [(e.key, e.id, dict(e.fields)) for e in resp.entries]

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
