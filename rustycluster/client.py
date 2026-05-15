"""
RustyCluster Python gRPC Client.

Main entry point for all key-value operations. Use get_client() to create
a fully initialized, authenticated client instance.

Example:
    from rustycluster import get_client, RustyClusterConfig

    config = RustyClusterConfig(
        nodes="localhost:50051,localhost:50052",
        username="admin", password="secret",
    )
    client = get_client(config)

    client.set("hello", "world")
    value = client.get("hello")   # "world"
    client.close()

    # Or use as a context manager:
    with get_client(config) as client:
        client.hset("user:1", "name", "Alice")
"""

from __future__ import annotations

import logging
from types import TracebackType
from typing import Any, Optional

import grpc

from rustycluster.config import RustyClusterConfig
from rustycluster.exceptions import (
    BatchOperationError,
    KeyNotFoundError,
    OperationError,
    from_grpc_error,
)
from rustycluster.failover import NodeManager
from rustycluster.proto import rustycluster_pb2, rustycluster_pb2_grpc
from rustycluster.retry import RetryPolicy

logger = logging.getLogger("rustycluster.client")


class _StubProxy:
    """
    Lazily resolves attribute access to the NodeManager's current stub.

    Each attribute lookup (e.g. `proxy.Set`) returns a thin wrapper function
    whose `__name__` is the gRPC method name. The retry layer reads
    `__name__` and re-binds the call to the latest stub on every attempt,
    which is how failover transparently swaps the underlying connection.
    """

    __slots__ = ("_manager",)

    def __init__(self, manager: NodeManager) -> None:
        self._manager = manager

    def __getattr__(self, name: str) -> Any:
        manager = self._manager

        def call(request: Any, **kwargs: Any) -> Any:
            return getattr(manager.stub, name)(request, **kwargs)

        call.__name__ = name
        return call


class RustyClusterClient:
    """
    Enterprise-grade Python client for RustyCluster distributed key-value store.

    Do not instantiate directly. Use get_client() which handles
    channel setup, authentication, and interceptor wiring.

    All methods return native Python types. Scan methods return
    (next_cursor: int, results) tuples. Operations that accept
    variadic arguments (sadd, srem, hdel) use *args syntax.
    """

    def __init__(
        self,
        manager: NodeManager,
        config: RustyClusterConfig,
    ) -> None:
        self._manager = manager
        self._config = config
        self._stub = _StubProxy(manager)
        self._retry = RetryPolicy(
            max_retries=config.max_retries,
            backoff_base=config.retry_backoff_base,
            backoff_max=config.retry_backoff_max,
            on_reauth=self.reauthenticate,
            stub_provider=lambda: manager.stub,
            on_node_failure=manager.failover,
        )

    # ──────────────────────────────────────────────
    # Context manager support
    # ──────────────────────────────────────────────

    def __enter__(self) -> "RustyClusterClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying gRPC channel and release resources."""
        self._manager.close()
        logger.debug("gRPC channel closed")

    # ──────────────────────────────────────────────
    # Auth
    # ──────────────────────────────────────────────

    def reauthenticate(self) -> None:
        """Re-authenticate and refresh the session token on the current node."""
        logger.info("Re-authenticating with RustyCluster server")
        self._manager.auth_manager.authenticate(timeout=self._config.timeout_seconds)

    # ──────────────────────────────────────────────
    # System operations
    # ──────────────────────────────────────────────

    def ping(self) -> bool:
        """Send a ping to the server. Returns True on success."""
        resp = self._retry.call(
            self._stub.Ping,
            rustycluster_pb2.PingRequest(),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def health_check(self) -> bool:
        """Perform a health check. Returns True if the server is healthy."""
        resp = self._retry.call(
            self._stub.HealthCheck,
            rustycluster_pb2.PingRequest(),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    # ──────────────────────────────────────────────
    # String operations
    # ──────────────────────────────────────────────

    def set(
        self,
        key: str,
        value: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """
        Set a key to a string value.

        Args:
            key: The key to set.
            value: The string value.
            skip_replication: Skip cluster replication for this write.
            skip_site_replication: Skip site replication for this write.

        Returns:
            True if the key was set successfully.
        """
        resp = self._retry.call(
            self._stub.Set,
            rustycluster_pb2.SetRequest(
                key=key,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def get(self, key: str) -> Optional[str]:
        """
        Get the value for a key.

        Args:
            key: The key to retrieve.

        Returns:
            The string value, or None if the key does not exist.
        """
        resp = self._retry.call(
            self._stub.Get,
            rustycluster_pb2.GetRequest(key=key),
            timeout=self._config.timeout_seconds,
        )
        return resp.value if resp.found else None

    def delete(
        self,
        key: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """
        Delete a key.

        Returns:
            True if the key was deleted.
        """
        resp = self._retry.call(
            self._stub.Delete,
            rustycluster_pb2.DeleteRequest(
                key=key,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def set_ex(
        self,
        key: str,
        value: str,
        ttl: int,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """Set a key to a value with an expiry (TTL in seconds)."""
        resp = self._retry.call(
            self._stub.SetEx,
            rustycluster_pb2.SetExRequest(
                key=key,
                value=value,
                ttl=ttl,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def set_expiry(
        self,
        key: str,
        ttl: int,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """Set or update the TTL on an existing key."""
        resp = self._retry.call(
            self._stub.SetExpiry,
            rustycluster_pb2.SetExpiryRequest(
                key=key,
                ttl=ttl,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def set_nx(
        self,
        key: str,
        value: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """
        Set a key only if it does not already exist.

        Returns:
            True if the key was set, False if it already existed.
        """
        resp = self._retry.call(
            self._stub.SetNX,
            rustycluster_pb2.SetNXRequest(
                key=key,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def exists(self, key: str) -> bool:
        """Return True if the key exists in the store."""
        resp = self._retry.call(
            self._stub.Exists,
            rustycluster_pb2.ExistsRequest(key=key),
            timeout=self._config.timeout_seconds,
        )
        return resp.exists

    # ──────────────────────────────────────────────
    # Numeric operations
    # ──────────────────────────────────────────────

    def incr_by(
        self,
        key: str,
        value: int,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """Increment the integer value of a key by the given amount."""
        resp = self._retry.call(
            self._stub.IncrBy,
            rustycluster_pb2.IncrByRequest(
                key=key,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.new_value

    def decr_by(
        self,
        key: str,
        value: int,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """Decrement the integer value of a key by the given amount."""
        resp = self._retry.call(
            self._stub.DecrBy,
            rustycluster_pb2.DecrByRequest(
                key=key,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.new_value

    def incr_by_float(
        self,
        key: str,
        value: float,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> float:
        """Increment the float value of a key by the given amount."""
        resp = self._retry.call(
            self._stub.IncrByFloat,
            rustycluster_pb2.IncrByFloatRequest(
                key=key,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.new_value

    # ──────────────────────────────────────────────
    # Hash operations
    # ──────────────────────────────────────────────

    def hset(
        self,
        key: str,
        field: str,
        value: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """Set a field in a hash."""
        resp = self._retry.call(
            self._stub.HSet,
            rustycluster_pb2.HSetRequest(
                key=key,
                field=field,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def hget(self, key: str, field: str) -> Optional[str]:
        """
        Get a field from a hash.

        Returns:
            The field value, or None if the key or field does not exist.
        """
        resp = self._retry.call(
            self._stub.HGet,
            rustycluster_pb2.HGetRequest(key=key, field=field),
            timeout=self._config.timeout_seconds,
        )
        return resp.value if resp.found else None

    def hget_all(self, key: str) -> dict[str, str]:
        """Get all field-value pairs from a hash."""
        resp = self._retry.call(
            self._stub.HGetAll,
            rustycluster_pb2.HGetAllRequest(key=key),
            timeout=self._config.timeout_seconds,
        )
        return dict(resp.fields)

    def hmset(
        self,
        key: str,
        fields: dict[str, str],
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """Set multiple fields in a hash at once."""
        resp = self._retry.call(
            self._stub.HMSet,
            rustycluster_pb2.HMSetRequest(
                key=key,
                fields=fields,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def hexists(self, key: str, field: str) -> bool:
        """Return True if the given field exists in the hash."""
        resp = self._retry.call(
            self._stub.HExists,
            rustycluster_pb2.HExistsRequest(key=key, field=field),
            timeout=self._config.timeout_seconds,
        )
        return resp.exists

    def hdel(
        self,
        key: str,
        *fields: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """
        Delete one or more fields from a hash.

        Returns:
            Number of fields that were deleted.
        """
        resp = self._retry.call(
            self._stub.HDel,
            rustycluster_pb2.HDelRequest(
                key=key,
                fields=list(fields),
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.deleted_count

    def hscan(
        self,
        key: str,
        cursor: int = 0,
        pattern: Optional[str] = None,
        count: Optional[int] = None,
    ) -> tuple[int, dict[str, str]]:
        """
        Incrementally iterate over a hash's fields.

        Args:
            key: The hash key.
            cursor: Cursor from the previous call (0 to start).
            pattern: Glob-style pattern to filter field names.
            count: Hint for approximate number of elements to return.

        Returns:
            Tuple of (next_cursor, {field: value, ...}).
            next_cursor is 0 when iteration is complete.
        """
        req = rustycluster_pb2.HScanRequest(key=key, cursor=cursor)
        if pattern is not None:
            req.pattern = pattern
        if count is not None:
            req.count = count

        resp = self._retry.call(self._stub.HScan, req, timeout=self._config.timeout_seconds)
        return resp.next_cursor, dict(resp.fields)

    def hlen(self, key: str) -> int:
        """Return the number of fields in a hash."""
        resp = self._retry.call(
            self._stub.HLen,
            rustycluster_pb2.HLenRequest(key=key),
            timeout=self._config.timeout_seconds,
        )
        return resp.length

    def hsetnx(
        self,
        key: str,
        field: str,
        value: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """
        Set a hash field only if it does not already exist.

        Returns:
            True if the field was set, False if it already existed.
        """
        resp = self._retry.call(
            self._stub.HSetNX,
            rustycluster_pb2.HSetNXRequest(
                key=key,
                field=field,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def hexpire(
        self,
        key: str,
        seconds: int,
        *fields: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """
        Set a TTL (in seconds) on specific fields of a hash.

        Returns:
            Number of fields that had their expiry set.
        """
        resp = self._retry.call(
            self._stub.HExpire,
            rustycluster_pb2.HExpireRequest(
                key=key,
                seconds=seconds,
                fields=list(fields),
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.expired_count

    def hincr_by(
        self,
        key: str,
        field: str,
        value: int,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """Increment a hash field's integer value."""
        resp = self._retry.call(
            self._stub.HIncrBy,
            rustycluster_pb2.HIncrByRequest(
                key=key,
                field=field,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.new_value

    def hdecr_by(
        self,
        key: str,
        field: str,
        value: int,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """Decrement a hash field's integer value."""
        resp = self._retry.call(
            self._stub.HDecrBy,
            rustycluster_pb2.HDecrByRequest(
                key=key,
                field=field,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.new_value

    def hincr_by_float(
        self,
        key: str,
        field: str,
        value: float,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> float:
        """Increment a hash field's float value."""
        resp = self._retry.call(
            self._stub.HIncrByFloat,
            rustycluster_pb2.HIncrByFloatRequest(
                key=key,
                field=field,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.new_value

    # ──────────────────────────────────────────────
    # Binary hash operations
    # ──────────────────────────────────────────────

    def hset_bytes(
        self,
        key: bytes,
        field: bytes,
        value: bytes,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """Set a hash field with binary-safe key, field, and value."""
        resp = self._retry.call(
            self._stub.HSetBytes,
            rustycluster_pb2.HSetBytesRequest(
                key=key,
                field=field,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def hget_bytes(self, key: bytes, field: bytes) -> Optional[bytes]:
        """Get a hash field as bytes. Returns None if not found."""
        resp = self._retry.call(
            self._stub.HGetBytes,
            rustycluster_pb2.HGetBytesRequest(key=key, field=field),
            timeout=self._config.timeout_seconds,
        )
        return resp.value if resp.found else None

    def hscan_bytes(
        self,
        key: bytes,
        cursor: int = 0,
        pattern: Optional[bytes] = None,
        count: Optional[int] = None,
    ) -> tuple[int, list[tuple[bytes, bytes]]]:
        """
        Incrementally iterate over a hash's binary fields.

        Returns:
            Tuple of (next_cursor, [(field_bytes, value_bytes), ...]).
        """
        req = rustycluster_pb2.HScanBytesRequest(key=key, cursor=cursor)
        if pattern is not None:
            req.pattern = pattern
        if count is not None:
            req.count = count

        resp = self._retry.call(self._stub.HScanBytes, req, timeout=self._config.timeout_seconds)
        return resp.next_cursor, [(f.field, f.value) for f in resp.fields]

    def hget_all_bytes(self, key: bytes) -> list[tuple[bytes, bytes]]:
        """Get all fields and values from a hash as bytes."""
        resp = self._retry.call(
            self._stub.HGetAllBytes,
            rustycluster_pb2.HGetAllBytesRequest(key=key),
            timeout=self._config.timeout_seconds,
        )
        return [(f.field, f.value) for f in resp.fields]

    # ──────────────────────────────────────────────
    # Set operations
    # ──────────────────────────────────────────────

    def sadd(
        self,
        key: str,
        *members: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """
        Add one or more members to a set.

        Returns:
            Number of members that were newly added (not already in the set).
        """
        resp = self._retry.call(
            self._stub.SAdd,
            rustycluster_pb2.SAddRequest(
                key=key,
                members=list(members),
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.added_count

    def smembers(self, key: str) -> list[str]:
        """Return all members of a set."""
        resp = self._retry.call(
            self._stub.SMembers,
            rustycluster_pb2.SMembersRequest(key=key),
            timeout=self._config.timeout_seconds,
        )
        return list(resp.members)

    def srem(
        self,
        key: str,
        *members: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """
        Remove one or more members from a set.

        Returns:
            Number of members that were removed.
        """
        resp = self._retry.call(
            self._stub.SRem,
            rustycluster_pb2.SRemRequest(
                key=key,
                members=list(members),
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.removed_count

    def scard(self, key: str) -> int:
        """Return the number of members in a set."""
        resp = self._retry.call(
            self._stub.SCard,
            rustycluster_pb2.SCardRequest(key=key),
            timeout=self._config.timeout_seconds,
        )
        return resp.cardinality

    def sscan(
        self,
        key: str,
        cursor: int = 0,
        pattern: Optional[str] = None,
        count: Optional[int] = None,
    ) -> tuple[int, list[str]]:
        """
        Incrementally iterate over a set's members.

        Returns:
            Tuple of (next_cursor, [member, ...]).
            next_cursor is 0 when iteration is complete.
        """
        req = rustycluster_pb2.SScanRequest(key=key, cursor=cursor)
        if pattern is not None:
            req.pattern = pattern
        if count is not None:
            req.count = count

        resp = self._retry.call(self._stub.SScan, req, timeout=self._config.timeout_seconds)
        return resp.cursor, list(resp.members)

    # ──────────────────────────────────────────────
    # Key operations
    # ──────────────────────────────────────────────

    def del_multiple(
        self,
        *keys: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """
        Delete multiple keys at once.

        Returns:
            Number of keys that were deleted.
        """
        resp = self._retry.call(
            self._stub.DelMultiple,
            rustycluster_pb2.DelMultipleRequest(
                keys=list(keys),
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.deleted_count

    def keys(self, pattern: str) -> list[str]:
        """
        Find all keys matching a pattern.

        Supports glob-style wildcards: *, ?, [].

        Args:
            pattern: Pattern to match (e.g., "user:*", "session:?????").

        Returns:
            List of matching key strings.
        """
        resp = self._retry.call(
            self._stub.Keys,
            rustycluster_pb2.KeysRequest(pattern=pattern),
            timeout=self._config.timeout_seconds,
        )
        return list(resp.keys)

    def mget(self, *keys: str) -> list[Optional[str]]:
        """
        Get values for multiple keys in a single call.

        Returns:
            List of values in the same order as the input keys.
            Elements are None if the corresponding key does not exist.
        """
        resp = self._retry.call(
            self._stub.MGet,
            rustycluster_pb2.MGetRequest(keys=list(keys)),
            timeout=self._config.timeout_seconds,
        )
        # Proto returns empty string for missing keys; normalize to None
        return [v if v != "" else None for v in resp.values]

    # ──────────────────────────────────────────────
    # List (queue) operations
    # ──────────────────────────────────────────────

    def lpush(
        self,
        key: str,
        *values: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """Prepend one or more values to a list. Returns new list length."""
        resp = self._retry.call(
            self._stub.LPush,
            rustycluster_pb2.LPushRequest(
                key=key,
                values=list(values),
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.length

    def rpush(
        self,
        key: str,
        *values: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """Append one or more values to a list. Returns new list length."""
        resp = self._retry.call(
            self._stub.RPush,
            rustycluster_pb2.RPushRequest(
                key=key,
                values=list(values),
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.length

    def lpushx(
        self,
        key: str,
        *values: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """Prepend values only if the list exists. Returns 0 if absent."""
        resp = self._retry.call(
            self._stub.LPushX,
            rustycluster_pb2.LPushXRequest(
                key=key,
                values=list(values),
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.length

    def rpushx(
        self,
        key: str,
        *values: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """Append values only if the list exists. Returns 0 if absent."""
        resp = self._retry.call(
            self._stub.RPushX,
            rustycluster_pb2.RPushXRequest(
                key=key,
                values=list(values),
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.length

    def lpop(
        self,
        key: str,
        count: Optional[int] = None,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> list[str]:
        """
        Pop one or more values from the head of a list.

        If count is None, pops a single element. Always returns a list
        (possibly empty if the key is missing or list is empty).
        """
        req = rustycluster_pb2.LPopRequest(
            key=key,
            skip_replication=skip_replication,
            skip_site_replication=skip_site_replication,
        )
        if count is not None:
            req.count = count
        resp = self._retry.call(
            self._stub.LPop, req, timeout=self._config.timeout_seconds
        )
        return list(resp.values)

    def rpop(
        self,
        key: str,
        count: Optional[int] = None,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> list[str]:
        """Pop one or more values from the tail of a list."""
        req = rustycluster_pb2.RPopRequest(
            key=key,
            skip_replication=skip_replication,
            skip_site_replication=skip_site_replication,
        )
        if count is not None:
            req.count = count
        resp = self._retry.call(
            self._stub.RPop, req, timeout=self._config.timeout_seconds
        )
        return list(resp.values)

    def lrange(self, key: str, start: int, stop: int) -> list[str]:
        """Read a slice of a list. `stop` is inclusive (Redis semantics)."""
        resp = self._retry.call(
            self._stub.LRange,
            rustycluster_pb2.LRangeRequest(key=key, start=start, stop=stop),
            timeout=self._config.timeout_seconds,
        )
        return list(resp.values)

    def llen(self, key: str) -> int:
        """Return the length of a list. Returns 0 if the key is missing."""
        resp = self._retry.call(
            self._stub.LLen,
            rustycluster_pb2.LLenRequest(key=key),
            timeout=self._config.timeout_seconds,
        )
        return resp.length

    def ltrim(
        self,
        key: str,
        start: int,
        stop: int,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """Trim a list to the given inclusive range."""
        resp = self._retry.call(
            self._stub.LTrim,
            rustycluster_pb2.LTrimRequest(
                key=key,
                start=start,
                stop=stop,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def lindex(self, key: str, index: int) -> Optional[str]:
        """Return the element at the given index, or None if out of range."""
        resp = self._retry.call(
            self._stub.LIndex,
            rustycluster_pb2.LIndexRequest(key=key, index=index),
            timeout=self._config.timeout_seconds,
        )
        return resp.value if resp.found else None

    def lset(
        self,
        key: str,
        index: int,
        value: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> bool:
        """Set the element at the given index. Errors if out of range."""
        resp = self._retry.call(
            self._stub.LSet,
            rustycluster_pb2.LSetRequest(
                key=key,
                index=index,
                value=value,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.success

    def lrem(
        self,
        key: str,
        count: int,
        element: str,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """
        Remove up to |count| elements equal to `element`.

        count > 0  -> remove from head; count < 0 -> remove from tail;
        count == 0 -> remove all matches. Returns number removed.
        """
        resp = self._retry.call(
            self._stub.LRem,
            rustycluster_pb2.LRemRequest(
                key=key,
                count=count,
                element=element,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.removed

    def linsert(
        self,
        key: str,
        pivot: str,
        element: str,
        before: bool = True,
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> int:
        """
        Insert `element` before/after the first occurrence of `pivot`.

        Returns the new list length, -1 if pivot not found, 0 if key absent.
        """
        resp = self._retry.call(
            self._stub.LInsert,
            rustycluster_pb2.LInsertRequest(
                key=key,
                before=before,
                pivot=pivot,
                element=element,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.length

    def lpos(
        self,
        key: str,
        element: str,
        rank: Optional[int] = None,
        count: Optional[int] = None,
    ) -> list[int]:
        """
        Find positions of `element` in the list.

        Args:
            rank: 1-based; negative searches from the tail.
            count: Maximum number of positions to return (None = at most one).
        """
        req = rustycluster_pb2.LPosRequest(key=key, element=element)
        if rank is not None:
            req.rank = rank
        if count is not None:
            req.count = count
        resp = self._retry.call(
            self._stub.LPos, req, timeout=self._config.timeout_seconds
        )
        return list(resp.positions)

    # ──────────────────────────────────────────────
    # Stream operations
    # ──────────────────────────────────────────────

    def xadd(
        self,
        key: str,
        fields: dict[str, str],
        id: str = "*",
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> str:
        """
        Append an entry to a stream. Pass id="*" to let the server assign one.

        Returns the resolved entry id.
        """
        resp = self._retry.call(
            self._stub.XAdd,
            rustycluster_pb2.XAddRequest(
                key=key,
                id=id,
                fields=fields,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        return resp.id

    def xread(
        self,
        streams: list[tuple[str, str]],
        count: Optional[int] = None,
    ) -> list[tuple[str, str, dict[str, str]]]:
        """
        Read entries from one or more streams (non-blocking).

        Args:
            streams: List of (key, last_id) pairs. Use "0" to read from the
                start, "$" for newest only.
            count: Optional cap on entries returned across all streams.

        Returns:
            List of (key, id, fields) tuples.
        """
        req = rustycluster_pb2.XReadRequest(
            streams=[
                rustycluster_pb2.XReadStreamKey(key=k, id=i) for k, i in streams
            ],
        )
        if count is not None:
            req.count = count
        resp = self._retry.call(
            self._stub.XRead, req, timeout=self._config.timeout_seconds
        )
        return [(e.key, e.id, dict(e.fields)) for e in resp.entries]

    # ──────────────────────────────────────────────
    # Script operations
    # ──────────────────────────────────────────────

    def load_script(self, script: str) -> str:
        """
        Load a Lua script into the server's script cache.

        Args:
            script: The Lua script source code.

        Returns:
            SHA1 hash of the loaded script (use with eval_sha).

        Raises:
            ScriptError: If the script could not be loaded.
        """
        from rustycluster.exceptions import ScriptError

        resp = self._retry.call(
            self._stub.LoadScript,
            rustycluster_pb2.LoadScriptRequest(script=script),
            timeout=self._config.timeout_seconds,
        )
        if not resp.success:
            raise ScriptError("Failed to load script into server cache")
        return resp.sha

    def eval_sha(
        self,
        sha: str,
        keys: list[str],
        args: list[str],
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> str:
        """
        Execute a previously loaded Lua script by its SHA.

        Args:
            sha: SHA1 hash returned by load_script().
            keys: List of key names the script will access.
            args: Additional arguments passed to the script.

        Returns:
            Script execution result as a string.

        Raises:
            ScriptError: If execution fails.
        """
        from rustycluster.exceptions import ScriptError

        resp = self._retry.call(
            self._stub.EvalSha,
            rustycluster_pb2.EvalShaRequest(
                sha=sha,
                keys=keys,
                args=args,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        if not resp.success:
            raise ScriptError(f"Script execution failed: {resp.result}")
        return resp.result

    # ──────────────────────────────────────────────
    # Batch operations
    # ──────────────────────────────────────────────

    def batch_write(
        self,
        operations: list[rustycluster_pb2.BatchOperation],
        skip_replication: bool = False,
        skip_site_replication: bool = False,
    ) -> list[bool]:
        """
        Execute a batch of write operations atomically.

        Build operations using BatchOperationBuilder for convenience:

            from rustycluster import BatchOperationBuilder as BOB
            ops = [BOB.set("k1", "v1"), BOB.hset("hash", "f1", "v1")]
            results = client.batch_write(ops)

        Args:
            operations: List of BatchOperation proto messages.
            skip_replication: Skip replication for all ops in this batch.
            skip_site_replication: Skip site replication for all ops.

        Returns:
            List of bool, one per operation indicating success.

        Raises:
            BatchOperationError: If the batch request itself fails.
        """
        resp = self._retry.call(
            self._stub.BatchWrite,
            rustycluster_pb2.BatchWriteRequest(
                operations=operations,
                skip_replication=skip_replication,
                skip_site_replication=skip_site_replication,
            ),
            timeout=self._config.timeout_seconds,
        )
        if not resp.success:
            raise BatchOperationError(
                "Batch write failed", results=list(resp.operation_results)
            )
        return list(resp.operation_results)
