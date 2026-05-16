"""
Builder helpers for constructing BatchOperation proto messages.

Usage:
    from rustycluster.batch import BatchOperationBuilder as BOB

    ops = [
        BOB.set("key1", "value1"),
        BOB.set_ex("key2", "value2", ttl=3600),
        BOB.delete("key3"),
        BOB.hset("user:1", "name", "Alice"),
        BOB.hmset("user:2", {"name": "Bob", "age": "30"}),
        BOB.sadd("tags", ["python", "grpc"]),
        BOB.incr_by("counter", 5),
    ]

    results = client.batch_write(ops)
"""

from __future__ import annotations

from rustycluster.proto import rustycluster_pb2

# Alias for brevity
_Op = rustycluster_pb2.BatchOperation
_OT = rustycluster_pb2.BatchOperation.OperationType


class BatchOperationBuilder:
    """Factory class for creating BatchOperation proto messages."""

    @staticmethod
    def set(key: str, value: str) -> _Op:
        """Create a SET operation."""
        return _Op(operation_type=_OT.SET, key=key, value=value)

    @staticmethod
    def delete(key: str) -> _Op:
        """Create a DELETE operation."""
        return _Op(operation_type=_OT.DELETE, key=key)

    @staticmethod
    def set_ex(key: str, value: str, ttl: int) -> _Op:
        """Create a SETEX operation with TTL."""
        return _Op(operation_type=_OT.SETEX, key=key, value=value, ttl=ttl)

    @staticmethod
    def set_expiry(key: str, ttl: int) -> _Op:
        """Create a SETEXPIRY operation."""
        return _Op(operation_type=_OT.SETEXPIRY, key=key, ttl=ttl)

    @staticmethod
    def set_nx(key: str, value: str) -> _Op:
        """Create a SETNX (set if not exists) operation."""
        return _Op(operation_type=_OT.SETNX, key=key, value=value)

    @staticmethod
    def incr_by(key: str, value: int) -> _Op:
        """Create an INCRBY operation."""
        return _Op(operation_type=_OT.INCRBY, key=key, int_value=value)

    @staticmethod
    def decr_by(key: str, value: int) -> _Op:
        """Create a DECRBY operation."""
        return _Op(operation_type=_OT.DECRBY, key=key, int_value=value)

    @staticmethod
    def incr_by_float(key: str, value: float) -> _Op:
        """Create an INCRBYFLOAT operation."""
        return _Op(operation_type=_OT.INCRBYFLOAT, key=key, float_value=value)

    @staticmethod
    def hset(key: str, field: str, value: str) -> _Op:
        """Create an HSET operation."""
        return _Op(operation_type=_OT.HSET, key=key, field=field, value=value)

    @staticmethod
    def hmset(key: str, fields: dict[str, str]) -> _Op:
        """Create an HMSET operation."""
        op = _Op(operation_type=_OT.HMSET, key=key)
        op.hash_fields.update(fields)
        return op

    @staticmethod
    def hdel(key: str, fields: list[str]) -> _Op:
        """Create an HDEL operation."""
        op = _Op(operation_type=_OT.HDEL, key=key)
        op.fields.extend(fields)
        return op

    @staticmethod
    def hsetnx(key: str, field: str, value: str) -> _Op:
        """Create an HSETNX operation."""
        return _Op(operation_type=_OT.HSETNX, key=key, field=field, value=value)

    @staticmethod
    def hexpire(key: str, seconds: int, fields: list[str]) -> _Op:
        """Create an HEXPIRE operation."""
        op = _Op(operation_type=_OT.HEXPIRE, key=key, seconds=seconds)
        op.fields.extend(fields)
        return op

    @staticmethod
    def hincrby(key: str, field: str, value: int) -> _Op:
        """Create an HINCRBY operation."""
        return _Op(operation_type=_OT.HINCRBY, key=key, field=field, int_value=value)

    @staticmethod
    def hdecrby(key: str, field: str, value: int) -> _Op:
        """Create an HDECRBY operation."""
        return _Op(operation_type=_OT.HDECRBY, key=key, field=field, int_value=value)

    @staticmethod
    def hincr_by_float(key: str, field: str, value: float) -> _Op:
        """Create an HINCRBYFLOAT operation."""
        return _Op(operation_type=_OT.HINCRBYFLOAT, key=key, field=field, float_value=value)

    @staticmethod
    def sadd(key: str, members: list[str]) -> _Op:
        """Create a SADD operation."""
        op = _Op(operation_type=_OT.SADD, key=key)
        op.members.extend(members)
        return op

    @staticmethod
    def srem(key: str, members: list[str]) -> _Op:
        """Create an SREM operation."""
        op = _Op(operation_type=_OT.SREM, key=key)
        op.members.extend(members)
        return op

    @staticmethod
    def del_multiple(keys: list[str]) -> _Op:
        """Create a DEL_MULTIPLE operation."""
        op = _Op(operation_type=_OT.DEL_MULTIPLE)
        op.keys.extend(keys)
        return op

    @staticmethod
    def eval_sha(sha: str, keys: list[str], args: list[str]) -> _Op:
        """Create an EVALSHA operation."""
        op = _Op(operation_type=_OT.EVALSHA, script_sha=sha)
        op.script_keys.extend(keys)
        op.script_args.extend(args)
        return op

    @staticmethod
    def load_script(script: str) -> _Op:
        """Create a LOAD_SCRIPT operation."""
        return _Op(operation_type=_OT.LOAD_SCRIPT, value=script)

    @staticmethod
    def hset_bytes(key: bytes, field: bytes, value: bytes) -> _Op:
        """Create an HSET_BYTES operation (binary safe)."""
        return _Op(
            operation_type=_OT.HSET_BYTES,
            bytes_key=key,
            bytes_field=field,
            bytes_value=value,
        )

    # List & stream batch builders. The proto's BatchOperation message gained
    # new OperationType enum values for list/stream ops but no dedicated
    # payload fields, so each builder reuses an existing slot:
    #   members      -> push values
    #   int_value    -> count / index
    #   value        -> element / stream id
    #   hash_fields  -> XAdd field map
    # LTRIM (needs two int64s), LINSERT (needs a bool + pivot + element),
    # and LPOS (read-only) cannot be expressed with the current fields and
    # are intentionally omitted until the proto adds the missing slots.

    @staticmethod
    def lpush(key: str, values: list[str]) -> _Op:
        """Create an LPUSH operation."""
        op = _Op(operation_type=_OT.LPUSH, key=key)
        op.members.extend(values)
        return op

    @staticmethod
    def rpush(key: str, values: list[str]) -> _Op:
        """Create an RPUSH operation."""
        op = _Op(operation_type=_OT.RPUSH, key=key)
        op.members.extend(values)
        return op

    @staticmethod
    def lpushx(key: str, values: list[str]) -> _Op:
        """Create an LPUSHX operation."""
        op = _Op(operation_type=_OT.LPUSHX, key=key)
        op.members.extend(values)
        return op

    @staticmethod
    def rpushx(key: str, values: list[str]) -> _Op:
        """Create an RPUSHX operation."""
        op = _Op(operation_type=_OT.RPUSHX, key=key)
        op.members.extend(values)
        return op

    @staticmethod
    def lpop(key: str, count: int | None = None) -> _Op:
        """Create an LPOP operation. Pass count to pop multiple elements."""
        op = _Op(operation_type=_OT.LPOP, key=key)
        if count is not None:
            op.int_value = count
        return op

    @staticmethod
    def rpop(key: str, count: int | None = None) -> _Op:
        """Create an RPOP operation. Pass count to pop multiple elements."""
        op = _Op(operation_type=_OT.RPOP, key=key)
        if count is not None:
            op.int_value = count
        return op

    @staticmethod
    def lset(key: str, index: int, value: str) -> _Op:
        """Create an LSET operation."""
        return _Op(operation_type=_OT.LSET, key=key, int_value=index, value=value)

    @staticmethod
    def lrem(key: str, count: int, element: str) -> _Op:
        """Create an LREM operation."""
        return _Op(operation_type=_OT.LREM, key=key, int_value=count, value=element)

    @staticmethod
    def xadd(key: str, fields: dict[str, str], id: str = "*") -> _Op:
        """Create an XADD operation. Pass id='*' for server-assigned id."""
        op = _Op(operation_type=_OT.XADD, key=key, value=id)
        op.hash_fields.update(fields)
        return op
