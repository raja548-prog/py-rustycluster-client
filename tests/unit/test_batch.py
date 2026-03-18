"""Unit tests for BatchOperationBuilder."""

from __future__ import annotations

import pytest

from rustycluster.batch import BatchOperationBuilder as BOB
from rustycluster.proto import rustycluster_pb2

_OT = rustycluster_pb2.BatchOperation.OperationType


class TestBatchOperationBuilder:
    def test_set(self):
        op = BOB.set("key1", "value1")
        assert op.operation_type == _OT.SET
        assert op.key == "key1"
        assert op.value == "value1"

    def test_delete(self):
        op = BOB.delete("key1")
        assert op.operation_type == _OT.DELETE
        assert op.key == "key1"

    def test_set_ex(self):
        op = BOB.set_ex("key1", "v", ttl=3600)
        assert op.operation_type == _OT.SETEX
        assert op.ttl == 3600

    def test_set_nx(self):
        op = BOB.set_nx("key1", "v")
        assert op.operation_type == _OT.SETNX

    def test_incr_by(self):
        op = BOB.incr_by("counter", 5)
        assert op.operation_type == _OT.INCRBY
        assert op.int_value == 5

    def test_decr_by(self):
        op = BOB.decr_by("counter", 3)
        assert op.operation_type == _OT.DECRBY
        assert op.int_value == 3

    def test_incr_by_float(self):
        op = BOB.incr_by_float("score", 1.5)
        assert op.operation_type == _OT.INCRBYFLOAT
        assert op.float_value == pytest.approx(1.5)

    def test_hset(self):
        op = BOB.hset("h", "field", "val")
        assert op.operation_type == _OT.HSET
        assert op.field == "field"

    def test_hmset(self):
        op = BOB.hmset("h", {"f1": "v1", "f2": "v2"})
        assert op.operation_type == _OT.HMSET
        assert dict(op.hash_fields) == {"f1": "v1", "f2": "v2"}

    def test_hdel(self):
        op = BOB.hdel("h", ["f1", "f2"])
        assert op.operation_type == _OT.HDEL
        assert list(op.fields) == ["f1", "f2"]

    def test_sadd(self):
        op = BOB.sadd("s", ["a", "b", "c"])
        assert op.operation_type == _OT.SADD
        assert list(op.members) == ["a", "b", "c"]

    def test_srem(self):
        op = BOB.srem("s", ["x"])
        assert op.operation_type == _OT.SREM

    def test_del_multiple(self):
        op = BOB.del_multiple(["k1", "k2"])
        assert op.operation_type == _OT.DEL_MULTIPLE
        assert list(op.keys) == ["k1", "k2"]

    def test_eval_sha(self):
        op = BOB.eval_sha("abc123", ["key1"], ["arg1"])
        assert op.operation_type == _OT.EVALSHA
        assert op.script_sha == "abc123"
        assert list(op.script_keys) == ["key1"]
        assert list(op.script_args) == ["arg1"]

    def test_hexpire(self):
        op = BOB.hexpire("h", 60, ["f1", "f2"])
        assert op.operation_type == _OT.HEXPIRE
        assert op.seconds == 60
        assert list(op.fields) == ["f1", "f2"]
