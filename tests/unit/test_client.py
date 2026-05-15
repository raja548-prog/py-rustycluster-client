"""Unit tests for RustyClusterClient methods using a mock stub."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rustycluster.client import RustyClusterClient
from rustycluster.config import RustyClusterConfig
from rustycluster.proto import rustycluster_pb2


def _make_client() -> tuple[RustyClusterClient, MagicMock]:
    """Return a client wired to a fully mocked stub via a fake NodeManager."""
    config = RustyClusterConfig(
        nodes="localhost:50051",
        username="admin", password="secret",
        max_retries=0,  # no retries in unit tests
    )
    stub = MagicMock()
    manager = MagicMock()
    manager.stub = stub
    manager.auth_manager = MagicMock()
    manager.auth_manager.get_token.return_value = "test-token"
    manager.failover.side_effect = RuntimeError(
        "failover should not trigger in unit tests"
    )
    client = RustyClusterClient(manager=manager, config=config)
    return client, stub


class TestStringOperations:
    def test_set_returns_true(self):
        client, stub = _make_client()
        stub.Set.return_value = rustycluster_pb2.SetResponse(success=True)
        assert client.set("k", "v") is True
        stub.Set.assert_called_once()

    def test_get_returns_value_when_found(self):
        client, stub = _make_client()
        stub.Get.return_value = rustycluster_pb2.GetResponse(value="hello", found=True)
        assert client.get("k") == "hello"

    def test_get_returns_none_when_not_found(self):
        client, stub = _make_client()
        stub.Get.return_value = rustycluster_pb2.GetResponse(value="", found=False)
        assert client.get("k") is None

    def test_delete_returns_true(self):
        client, stub = _make_client()
        stub.Delete.return_value = rustycluster_pb2.DeleteResponse(success=True)
        assert client.delete("k") is True

    def test_set_ex(self):
        client, stub = _make_client()
        stub.SetEx.return_value = rustycluster_pb2.SetExResponse(success=True)
        assert client.set_ex("k", "v", ttl=3600) is True

    def test_set_nx_true_when_key_new(self):
        client, stub = _make_client()
        stub.SetNX.return_value = rustycluster_pb2.SetNXResponse(success=True)
        assert client.set_nx("k", "v") is True

    def test_set_nx_false_when_key_exists(self):
        client, stub = _make_client()
        stub.SetNX.return_value = rustycluster_pb2.SetNXResponse(success=False)
        assert client.set_nx("k", "v") is False

    def test_exists_true(self):
        client, stub = _make_client()
        stub.Exists.return_value = rustycluster_pb2.ExistsResponse(exists=True)
        assert client.exists("k") is True


class TestNumericOperations:
    def test_incr_by(self):
        client, stub = _make_client()
        stub.IncrBy.return_value = rustycluster_pb2.IncrByResponse(new_value=10)
        assert client.incr_by("counter", 3) == 10

    def test_decr_by(self):
        client, stub = _make_client()
        stub.DecrBy.return_value = rustycluster_pb2.DecrByResponse(new_value=7)
        assert client.decr_by("counter", 3) == 7

    def test_incr_by_float(self):
        client, stub = _make_client()
        stub.IncrByFloat.return_value = rustycluster_pb2.IncrByFloatResponse(new_value=3.14)
        assert client.incr_by_float("score", 1.5) == pytest.approx(3.14)


class TestHashOperations:
    def test_hset(self):
        client, stub = _make_client()
        stub.HSet.return_value = rustycluster_pb2.HSetResponse(success=True)
        assert client.hset("h", "f", "v") is True

    def test_hget_returns_value(self):
        client, stub = _make_client()
        stub.HGet.return_value = rustycluster_pb2.HGetResponse(value="val", found=True)
        assert client.hget("h", "f") == "val"

    def test_hget_returns_none_when_missing(self):
        client, stub = _make_client()
        stub.HGet.return_value = rustycluster_pb2.HGetResponse(value="", found=False)
        assert client.hget("h", "f") is None

    def test_hget_all(self):
        client, stub = _make_client()
        resp = rustycluster_pb2.HGetAllResponse()
        resp.fields["name"] = "Alice"
        resp.fields["age"] = "30"
        stub.HGetAll.return_value = resp
        result = client.hget_all("user:1")
        assert result == {"name": "Alice", "age": "30"}

    def test_hdel_returns_count(self):
        client, stub = _make_client()
        stub.HDel.return_value = rustycluster_pb2.HDelResponse(deleted_count=2)
        assert client.hdel("h", "f1", "f2") == 2

    def test_hlen(self):
        client, stub = _make_client()
        stub.HLen.return_value = rustycluster_pb2.HLenResponse(length=5)
        assert client.hlen("h") == 5

    def test_hscan_returns_cursor_and_fields(self):
        client, stub = _make_client()
        resp = rustycluster_pb2.HScanResponse(next_cursor=42)
        resp.fields["f1"] = "v1"
        stub.HScan.return_value = resp
        cursor, fields = client.hscan("h", cursor=0)
        assert cursor == 42
        assert fields == {"f1": "v1"}

    def test_hexists_true(self):
        client, stub = _make_client()
        stub.HExists.return_value = rustycluster_pb2.HExistsResponse(exists=True)
        assert client.hexists("h", "f") is True

    def test_hsetnx_true_when_new(self):
        client, stub = _make_client()
        stub.HSetNX.return_value = rustycluster_pb2.HSetNXResponse(success=True)
        assert client.hsetnx("h", "f", "v") is True

    def test_hexpire_returns_count(self):
        client, stub = _make_client()
        stub.HExpire.return_value = rustycluster_pb2.HExpireResponse(expired_count=3)
        assert client.hexpire("h", 60, "f1", "f2", "f3") == 3


class TestSetOperations:
    def test_sadd_returns_count(self):
        client, stub = _make_client()
        stub.SAdd.return_value = rustycluster_pb2.SAddResponse(added_count=2)
        assert client.sadd("s", "a", "b") == 2

    def test_smembers(self):
        client, stub = _make_client()
        stub.SMembers.return_value = rustycluster_pb2.SMembersResponse(members=["a", "b", "c"])
        assert sorted(client.smembers("s")) == ["a", "b", "c"]

    def test_srem_returns_count(self):
        client, stub = _make_client()
        stub.SRem.return_value = rustycluster_pb2.SRemResponse(removed_count=1)
        assert client.srem("s", "a") == 1

    def test_scard(self):
        client, stub = _make_client()
        stub.SCard.return_value = rustycluster_pb2.SCardResponse(cardinality=5)
        assert client.scard("s") == 5

    def test_sscan_returns_cursor_and_members(self):
        client, stub = _make_client()
        stub.SScan.return_value = rustycluster_pb2.SScanResponse(cursor=10, members=["x", "y"])
        cursor, members = client.sscan("s", cursor=0)
        assert cursor == 10
        assert members == ["x", "y"]


class TestKeyOperations:
    def test_del_multiple_returns_count(self):
        client, stub = _make_client()
        stub.DelMultiple.return_value = rustycluster_pb2.DelMultipleResponse(deleted_count=3)
        assert client.del_multiple("k1", "k2", "k3") == 3

    def test_keys_returns_list(self):
        client, stub = _make_client()
        stub.Keys.return_value = rustycluster_pb2.KeysResponse(keys=["user:1", "user:2"])
        assert client.keys("user:*") == ["user:1", "user:2"]

    def test_mget_returns_values_with_none_for_missing(self):
        client, stub = _make_client()
        stub.MGet.return_value = rustycluster_pb2.MGetResponse(values=["v1", "", "v3"])
        result = client.mget("k1", "k2", "k3")
        assert result == ["v1", None, "v3"]


class TestSystemOperations:
    def test_ping_true(self):
        client, stub = _make_client()
        stub.Ping.return_value = rustycluster_pb2.PingResponse(success=True)
        assert client.ping() is True

    def test_health_check_true(self):
        client, stub = _make_client()
        stub.HealthCheck.return_value = rustycluster_pb2.PingResponse(success=True)
        assert client.health_check() is True


class TestListOperations:
    def test_lpush_returns_length_and_passes_values(self):
        client, stub = _make_client()
        stub.LPush.return_value = rustycluster_pb2.LPushResponse(length=3)
        assert client.lpush("q", "a", "b", "c") == 3
        req = stub.LPush.call_args.args[0]
        assert req.key == "q"
        assert list(req.values) == ["a", "b", "c"]

    def test_rpush_returns_length(self):
        client, stub = _make_client()
        stub.RPush.return_value = rustycluster_pb2.RPushResponse(length=2)
        assert client.rpush("q", "x", "y") == 2

    def test_lpushx_returns_zero_when_absent(self):
        client, stub = _make_client()
        stub.LPushX.return_value = rustycluster_pb2.LPushXResponse(length=0)
        assert client.lpushx("q", "v") == 0

    def test_rpushx_returns_length(self):
        client, stub = _make_client()
        stub.RPushX.return_value = rustycluster_pb2.RPushXResponse(length=4)
        assert client.rpushx("q", "v") == 4

    def test_lpop_no_count_does_not_set_count(self):
        client, stub = _make_client()
        stub.LPop.return_value = rustycluster_pb2.LPopResponse(values=["a"])
        assert client.lpop("q") == ["a"]
        req = stub.LPop.call_args.args[0]
        assert req.HasField("count") is False

    def test_lpop_with_count_sets_count(self):
        client, stub = _make_client()
        stub.LPop.return_value = rustycluster_pb2.LPopResponse(values=["a", "b"])
        assert client.lpop("q", count=2) == ["a", "b"]
        req = stub.LPop.call_args.args[0]
        assert req.count == 2

    def test_lpop_empty_returns_empty_list(self):
        client, stub = _make_client()
        stub.LPop.return_value = rustycluster_pb2.LPopResponse(values=[])
        assert client.lpop("q") == []

    def test_rpop_with_count(self):
        client, stub = _make_client()
        stub.RPop.return_value = rustycluster_pb2.RPopResponse(values=["z", "y"])
        assert client.rpop("q", count=2) == ["z", "y"]

    def test_lrange_returns_list(self):
        client, stub = _make_client()
        stub.LRange.return_value = rustycluster_pb2.LRangeResponse(values=["a", "b", "c"])
        assert client.lrange("q", 0, -1) == ["a", "b", "c"]

    def test_llen_zero_when_missing(self):
        client, stub = _make_client()
        stub.LLen.return_value = rustycluster_pb2.LLenResponse(length=0)
        assert client.llen("missing") == 0

    def test_ltrim_returns_true(self):
        client, stub = _make_client()
        stub.LTrim.return_value = rustycluster_pb2.LTrimResponse(success=True)
        assert client.ltrim("q", 0, 9) is True

    def test_lindex_found(self):
        client, stub = _make_client()
        stub.LIndex.return_value = rustycluster_pb2.LIndexResponse(found=True, value="x")
        assert client.lindex("q", 0) == "x"

    def test_lindex_missing_returns_none(self):
        client, stub = _make_client()
        stub.LIndex.return_value = rustycluster_pb2.LIndexResponse(found=False, value="")
        assert client.lindex("q", 99) is None

    def test_lset_returns_true(self):
        client, stub = _make_client()
        stub.LSet.return_value = rustycluster_pb2.LSetResponse(success=True)
        assert client.lset("q", 0, "v") is True

    def test_lrem_positive_count(self):
        client, stub = _make_client()
        stub.LRem.return_value = rustycluster_pb2.LRemResponse(removed=2)
        assert client.lrem("q", 2, "a") == 2
        req = stub.LRem.call_args.args[0]
        assert req.count == 2

    def test_lrem_negative_count(self):
        client, stub = _make_client()
        stub.LRem.return_value = rustycluster_pb2.LRemResponse(removed=1)
        assert client.lrem("q", -1, "a") == 1
        assert stub.LRem.call_args.args[0].count == -1

    def test_lrem_zero_removes_all(self):
        client, stub = _make_client()
        stub.LRem.return_value = rustycluster_pb2.LRemResponse(removed=5)
        assert client.lrem("q", 0, "a") == 5
        assert stub.LRem.call_args.args[0].count == 0

    def test_linsert_default_before_is_true(self):
        client, stub = _make_client()
        stub.LInsert.return_value = rustycluster_pb2.LInsertResponse(length=4)
        assert client.linsert("q", "pivot", "el") == 4
        assert stub.LInsert.call_args.args[0].before is True

    def test_linsert_after_passes_false(self):
        client, stub = _make_client()
        stub.LInsert.return_value = rustycluster_pb2.LInsertResponse(length=4)
        client.linsert("q", "pivot", "el", before=False)
        assert stub.LInsert.call_args.args[0].before is False

    def test_lpos_no_optionals_returns_positions(self):
        client, stub = _make_client()
        stub.LPos.return_value = rustycluster_pb2.LPosResponse(positions=[2])
        assert client.lpos("q", "a") == [2]
        req = stub.LPos.call_args.args[0]
        assert req.HasField("rank") is False
        assert req.HasField("count") is False

    def test_lpos_with_rank_and_count(self):
        client, stub = _make_client()
        stub.LPos.return_value = rustycluster_pb2.LPosResponse(positions=[2, 4])
        assert client.lpos("q", "a", rank=1, count=2) == [2, 4]
        req = stub.LPos.call_args.args[0]
        assert req.rank == 1
        assert req.count == 2

    def test_lpos_no_match_returns_empty(self):
        client, stub = _make_client()
        stub.LPos.return_value = rustycluster_pb2.LPosResponse(positions=[])
        assert client.lpos("q", "z") == []


class TestStreamOperations:
    def test_xadd_default_id_is_star_and_returns_resolved(self):
        client, stub = _make_client()
        stub.XAdd.return_value = rustycluster_pb2.XAddResponse(id="1700000000000-1")
        assert client.xadd("s", {"f": "v"}) == "1700000000000-1"
        req = stub.XAdd.call_args.args[0]
        assert req.id == "*"
        assert dict(req.fields) == {"f": "v"}

    def test_xadd_custom_id_passes_through(self):
        client, stub = _make_client()
        stub.XAdd.return_value = rustycluster_pb2.XAddResponse(id="42-0")
        client.xadd("s", {"f": "v"}, id="42-0")
        assert stub.XAdd.call_args.args[0].id == "42-0"

    def test_xread_returns_list_of_tuples(self):
        client, stub = _make_client()
        resp = rustycluster_pb2.XReadResponse()
        e1 = resp.entries.add(key="s", id="1-0")
        e1.fields["f"] = "v"
        e2 = resp.entries.add(key="s", id="2-0")
        stub.XRead.return_value = resp
        result = client.xread([("s", "0")])
        assert result == [("s", "1-0", {"f": "v"}), ("s", "2-0", {})]

    def test_xread_with_count_sets_count(self):
        client, stub = _make_client()
        stub.XRead.return_value = rustycluster_pb2.XReadResponse()
        client.xread([("s", "0")], count=5)
        req = stub.XRead.call_args.args[0]
        assert req.count == 5

    def test_xread_no_count_does_not_set_count(self):
        client, stub = _make_client()
        stub.XRead.return_value = rustycluster_pb2.XReadResponse()
        client.xread([("s", "0")])
        req = stub.XRead.call_args.args[0]
        assert req.HasField("count") is False

    def test_xread_empty_returns_empty_list(self):
        client, stub = _make_client()
        stub.XRead.return_value = rustycluster_pb2.XReadResponse()
        assert client.xread([("s", "$")]) == []


class TestBatchOperations:
    def test_batch_write_success(self):
        client, stub = _make_client()
        stub.BatchWrite.return_value = rustycluster_pb2.BatchWriteResponse(
            success=True,
            operation_results=[True, True, False],
        )
        from rustycluster.batch import BatchOperationBuilder as BOB
        ops = [BOB.set("k1", "v1"), BOB.delete("k2"), BOB.set("k3", "v3")]
        results = client.batch_write(ops)
        assert results == [True, True, False]

    def test_batch_write_raises_on_failure(self):
        from rustycluster.exceptions import BatchOperationError
        client, stub = _make_client()
        stub.BatchWrite.return_value = rustycluster_pb2.BatchWriteResponse(
            success=False,
            operation_results=[True, False],
        )
        from rustycluster.batch import BatchOperationBuilder as BOB
        with pytest.raises(BatchOperationError):
            client.batch_write([BOB.set("k", "v")])


class TestContextManager:
    def test_close_called_on_exit(self):
        client, stub = _make_client()
        with client:
            pass
        client._manager.close.assert_called_once()
