"""
Integration test template using a mock gRPC server.

These tests spin up a local gRPC server that implements KeyValueServiceServicer,
so they test the full client-server round-trip without a real RustyCluster instance.

Run with:
    pytest tests/integration/ -v
"""

from __future__ import annotations

import threading
import time
from concurrent import futures
from unittest.mock import MagicMock

import grpc
import pytest

from rustycluster.proto import rustycluster_pb2, rustycluster_pb2_grpc


class MockKeyValueServicer(rustycluster_pb2_grpc.KeyValueServiceServicer):
    """In-process mock RustyCluster server for integration testing."""

    def __init__(self):
        self._store: dict[str, str] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        self._sets: dict[str, set[str]] = {}
        self._lists: dict[str, list[str]] = {}
        self._streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self._sorted_sets: dict[str, dict[str, float]] = {}  # key -> {member: score}
        self._stream_seq: int = 0
        self._session_token = "test-session-token-abc123"

    def Authenticate(self, request, context):
        if request.username == "admin" and request.creds == "secret":
            return rustycluster_pb2.AuthenticateResponse(
                success=True,
                session_token=self._session_token,
                message="OK",
            )
        context.set_code(grpc.StatusCode.UNAUTHENTICATED)
        context.set_details("Invalid credentials")
        return rustycluster_pb2.AuthenticateResponse(success=False, message="Invalid credentials")

    def Ping(self, request, context):
        return rustycluster_pb2.PingResponse(success=True, message="PONG", latency_ms=0)

    def HealthCheck(self, request, context):
        return rustycluster_pb2.PingResponse(success=True, message="OK", latency_ms=0)

    def Set(self, request, context):
        self._store[request.key] = request.value
        return rustycluster_pb2.SetResponse(success=True)

    def Get(self, request, context):
        if request.key in self._store:
            return rustycluster_pb2.GetResponse(value=self._store[request.key], found=True)
        return rustycluster_pb2.GetResponse(value="", found=False)

    def Delete(self, request, context):
        existed = request.key in self._store
        self._store.pop(request.key, None)
        return rustycluster_pb2.DeleteResponse(success=existed)

    def SetEx(self, request, context):
        self._store[request.key] = request.value
        return rustycluster_pb2.SetExResponse(success=True)

    def SetExpiry(self, request, context):
        return rustycluster_pb2.SetExpiryResponse(success=request.key in self._store)

    def SetNX(self, request, context):
        if request.key not in self._store:
            self._store[request.key] = request.value
            return rustycluster_pb2.SetNXResponse(success=True)
        return rustycluster_pb2.SetNXResponse(success=False)

    def Exists(self, request, context):
        return rustycluster_pb2.ExistsResponse(exists=request.key in self._store)

    def IncrBy(self, request, context):
        current = int(self._store.get(request.key, "0"))
        new_val = current + request.value
        self._store[request.key] = str(new_val)
        return rustycluster_pb2.IncrByResponse(new_value=new_val)

    def DecrBy(self, request, context):
        current = int(self._store.get(request.key, "0"))
        new_val = current - request.value
        self._store[request.key] = str(new_val)
        return rustycluster_pb2.DecrByResponse(new_value=new_val)

    def IncrByFloat(self, request, context):
        current = float(self._store.get(request.key, "0.0"))
        new_val = current + request.value
        self._store[request.key] = str(new_val)
        return rustycluster_pb2.IncrByFloatResponse(new_value=new_val)

    def HSet(self, request, context):
        self._hashes.setdefault(request.key, {})[request.field] = request.value
        return rustycluster_pb2.HSetResponse(success=True)

    def HGet(self, request, context):
        h = self._hashes.get(request.key, {})
        if request.field in h:
            return rustycluster_pb2.HGetResponse(value=h[request.field], found=True)
        return rustycluster_pb2.HGetResponse(value="", found=False)

    def HGetAll(self, request, context):
        resp = rustycluster_pb2.HGetAllResponse()
        resp.fields.update(self._hashes.get(request.key, {}))
        return resp

    def HMSet(self, request, context):
        self._hashes.setdefault(request.key, {}).update(request.fields)
        return rustycluster_pb2.HMSetResponse(success=True)

    def HExists(self, request, context):
        exists = request.field in self._hashes.get(request.key, {})
        return rustycluster_pb2.HExistsResponse(exists=exists)

    def HDel(self, request, context):
        h = self._hashes.get(request.key, {})
        count = sum(1 for f in request.fields if h.pop(f, None) is not None)
        return rustycluster_pb2.HDelResponse(deleted_count=count)

    def HLen(self, request, context):
        return rustycluster_pb2.HLenResponse(length=len(self._hashes.get(request.key, {})))

    def HSetNX(self, request, context):
        h = self._hashes.setdefault(request.key, {})
        if request.field not in h:
            h[request.field] = request.value
            return rustycluster_pb2.HSetNXResponse(success=True)
        return rustycluster_pb2.HSetNXResponse(success=False)

    def HExpire(self, request, context):
        h = self._hashes.get(request.key, {})
        count = sum(1 for f in request.fields if f in h)
        return rustycluster_pb2.HExpireResponse(expired_count=count)

    def SAdd(self, request, context):
        s = self._sets.setdefault(request.key, set())
        before = len(s)
        s.update(request.members)
        return rustycluster_pb2.SAddResponse(added_count=len(s) - before)

    def SMembers(self, request, context):
        return rustycluster_pb2.SMembersResponse(
            members=list(self._sets.get(request.key, set()))
        )

    def SRem(self, request, context):
        s = self._sets.get(request.key, set())
        count = sum(1 for m in request.members if m in s and s.discard(m) is None)
        return rustycluster_pb2.SRemResponse(removed_count=count)

    def SCard(self, request, context):
        return rustycluster_pb2.SCardResponse(cardinality=len(self._sets.get(request.key, set())))

    def SScan(self, request, context):
        members = list(self._sets.get(request.key, set()))
        return rustycluster_pb2.SScanResponse(cursor=0, members=members)

    def DelMultiple(self, request, context):
        count = sum(1 for k in request.keys if self._store.pop(k, None) is not None)
        return rustycluster_pb2.DelMultipleResponse(deleted_count=count)

    def Keys(self, request, context):
        import fnmatch
        matched = [k for k in self._store if fnmatch.fnmatch(k, request.pattern)]
        return rustycluster_pb2.KeysResponse(keys=matched)

    def MGet(self, request, context):
        values = [self._store.get(k, "") for k in request.keys]
        return rustycluster_pb2.MGetResponse(values=values)

    def LoadScript(self, request, context):
        import hashlib
        sha = hashlib.sha1(request.script.encode()).hexdigest()
        return rustycluster_pb2.LoadScriptResponse(success=True, sha=sha)

    def EvalSha(self, request, context):
        return rustycluster_pb2.EvalShaResponse(success=True, result="script_result")

    # ─── List operations ───

    def LPush(self, request, context):
        lst = self._lists.setdefault(request.key, [])
        for v in request.values:
            lst.insert(0, v)
        return rustycluster_pb2.LPushResponse(length=len(lst))

    def RPush(self, request, context):
        lst = self._lists.setdefault(request.key, [])
        lst.extend(request.values)
        return rustycluster_pb2.RPushResponse(length=len(lst))

    def LPushX(self, request, context):
        if request.key not in self._lists:
            return rustycluster_pb2.LPushXResponse(length=0)
        lst = self._lists[request.key]
        for v in request.values:
            lst.insert(0, v)
        return rustycluster_pb2.LPushXResponse(length=len(lst))

    def RPushX(self, request, context):
        if request.key not in self._lists:
            return rustycluster_pb2.RPushXResponse(length=0)
        lst = self._lists[request.key]
        lst.extend(request.values)
        return rustycluster_pb2.RPushXResponse(length=len(lst))

    def _pop(self, key, count, from_left):
        lst = self._lists.get(key)
        if not lst:
            return []
        n = count if count > 0 else 1
        n = min(n, len(lst))
        if from_left:
            popped = lst[:n]
            del lst[:n]
        else:
            popped = list(reversed(lst[-n:]))
            del lst[-n:]
        return popped

    def LPop(self, request, context):
        count = request.count if request.HasField("count") else 0
        return rustycluster_pb2.LPopResponse(
            values=self._pop(request.key, count, from_left=True)
        )

    def RPop(self, request, context):
        count = request.count if request.HasField("count") else 0
        return rustycluster_pb2.RPopResponse(
            values=self._pop(request.key, count, from_left=False)
        )

    def LRange(self, request, context):
        lst = self._lists.get(request.key, [])
        stop = request.stop
        # Redis-style inclusive stop; -1 means "to the end"
        end = None if stop == -1 else stop + 1
        return rustycluster_pb2.LRangeResponse(values=lst[request.start:end])

    def LLen(self, request, context):
        return rustycluster_pb2.LLenResponse(length=len(self._lists.get(request.key, [])))

    def LTrim(self, request, context):
        if request.key in self._lists:
            stop = request.stop
            end = None if stop == -1 else stop + 1
            self._lists[request.key] = self._lists[request.key][request.start:end]
        return rustycluster_pb2.LTrimResponse(success=True)

    def LIndex(self, request, context):
        lst = self._lists.get(request.key, [])
        idx = request.index
        if -len(lst) <= idx < len(lst):
            return rustycluster_pb2.LIndexResponse(found=True, value=lst[idx])
        return rustycluster_pb2.LIndexResponse(found=False, value="")

    def LSet(self, request, context):
        lst = self._lists.get(request.key)
        idx = request.index
        if lst is None or not (-len(lst) <= idx < len(lst)):
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            context.set_details("index out of range")
            return rustycluster_pb2.LSetResponse(success=False)
        lst[idx] = request.value
        return rustycluster_pb2.LSetResponse(success=True)

    def LRem(self, request, context):
        lst = self._lists.get(request.key)
        if not lst:
            return rustycluster_pb2.LRemResponse(removed=0)
        count = request.count
        element = request.element
        removed = 0
        if count == 0:
            new_lst = [x for x in lst if x != element]
            removed = len(lst) - len(new_lst)
            self._lists[request.key] = new_lst
        elif count > 0:
            new_lst = []
            for x in lst:
                if removed < count and x == element:
                    removed += 1
                else:
                    new_lst.append(x)
            self._lists[request.key] = new_lst
        else:  # count < 0 -> from tail
            cap = -count
            new_rev = []
            for x in reversed(lst):
                if removed < cap and x == element:
                    removed += 1
                else:
                    new_rev.append(x)
            self._lists[request.key] = list(reversed(new_rev))
        return rustycluster_pb2.LRemResponse(removed=removed)

    def LInsert(self, request, context):
        if request.key not in self._lists:
            return rustycluster_pb2.LInsertResponse(length=0)
        lst = self._lists[request.key]
        try:
            idx = lst.index(request.pivot)
        except ValueError:
            return rustycluster_pb2.LInsertResponse(length=-1)
        insert_at = idx if request.before else idx + 1
        lst.insert(insert_at, request.element)
        return rustycluster_pb2.LInsertResponse(length=len(lst))

    def LPos(self, request, context):
        lst = self._lists.get(request.key, [])
        element = request.element
        rank = request.rank if request.HasField("rank") else 1
        count = request.count if request.HasField("count") else 1
        max_results = 0 if count == 0 else count  # 0 means "all"
        if rank == 0:
            return rustycluster_pb2.LPosResponse(positions=[])
        # Build the search iterator
        indices = range(len(lst)) if rank > 0 else range(len(lst) - 1, -1, -1)
        skip = abs(rank) - 1
        matches: list[int] = []
        for i in indices:
            if lst[i] == element:
                if skip > 0:
                    skip -= 1
                    continue
                matches.append(i)
                if max_results and len(matches) >= max_results:
                    break
        return rustycluster_pb2.LPosResponse(positions=matches)

    # ─── Sorted set operations ───

    def ZAdd(self, request, context):
        zset = self._sorted_sets.setdefault(request.key, {})
        is_new = request.member not in zset
        zset[request.member] = request.score
        return rustycluster_pb2.ZAddResponse(added=1 if is_new else 0)

    def ZRem(self, request, context):
        zset = self._sorted_sets.get(request.key, {})
        removed = sum(1 for m in request.members if zset.pop(m, None) is not None)
        return rustycluster_pb2.ZRemResponse(removed=removed)

    def ZRangeByScore(self, request, context):
        zset = self._sorted_sets.get(request.key, {})
        items = sorted(
            ((m, s) for m, s in zset.items() if request.min <= s <= request.max),
            key=lambda x: x[1],
        )
        if request.has_limit:
            items = items[request.offset: request.offset + request.count]
        resp = rustycluster_pb2.ZRangeByScoreResponse()
        for member, score in items:
            resp.members.add(member=member, score=score)
        return resp

    def ZRange(self, request, context):
        zset = self._sorted_sets.get(request.key, {})
        items = sorted(zset.items(), key=lambda x: x[1])
        stop = request.stop
        end = None if stop == -1 else stop + 1
        items = items[request.start:end]
        resp = rustycluster_pb2.ZRangeResponse()
        for member, score in items:
            resp.members.add(member=member, score=score)
        return resp

    def ZScore(self, request, context):
        zset = self._sorted_sets.get(request.key, {})
        if request.member in zset:
            return rustycluster_pb2.ZScoreResponse(found=True, score=zset[request.member])
        return rustycluster_pb2.ZScoreResponse(found=False, score=0.0)

    def ZCard(self, request, context):
        return rustycluster_pb2.ZCardResponse(
            cardinality=len(self._sorted_sets.get(request.key, {}))
        )

    # ─── Blocking list operations ───

    def BLPop(self, request, context):
        for key in request.keys:
            lst = self._lists.get(key)
            if lst:
                value = lst.pop(0)
                return rustycluster_pb2.BLPopResponse(key=key, value=value)
        return rustycluster_pb2.BLPopResponse(key="", value="")

    def BRPop(self, request, context):
        for key in request.keys:
            lst = self._lists.get(key)
            if lst:
                value = lst.pop()
                return rustycluster_pb2.BRPopResponse(key=key, value=value)
        return rustycluster_pb2.BRPopResponse(key="", value="")

    # ─── Stream operations ───

    def _next_stream_id(self) -> str:
        self._stream_seq += 1
        return f"{int(time.time() * 1000)}-{self._stream_seq}"

    def XAdd(self, request, context):
        resolved = self._next_stream_id() if request.id == "*" else request.id
        self._streams.setdefault(request.key, []).append((resolved, dict(request.fields)))
        return rustycluster_pb2.XAddResponse(id=resolved)

    def XRead(self, request, context):
        max_count = request.count if request.HasField("count") else 0
        entries: list[rustycluster_pb2.XReadEntry] = []
        for s in request.streams:
            for entry_id, fields in self._streams.get(s.key, []):
                if entry_id > s.id:
                    e = rustycluster_pb2.XReadEntry(key=s.key, id=entry_id)
                    e.fields.update(fields)
                    entries.append(e)
                    if max_count and len(entries) >= max_count:
                        break
            if max_count and len(entries) >= max_count:
                break
        return rustycluster_pb2.XReadResponse(entries=entries)

    def BatchWrite(self, request, context):
        results = []
        _OT = rustycluster_pb2.BatchOperation.OperationType
        for op in request.operations:
            if op.operation_type == _OT.SET:
                self._store[op.key] = op.value
                results.append(True)
            elif op.operation_type == _OT.DELETE:
                self._store.pop(op.key, None)
                results.append(True)
            elif op.operation_type == _OT.HSET:
                self._hashes.setdefault(op.key, {})[op.field] = op.value
                results.append(True)
            elif op.operation_type == _OT.LPUSH:
                lst = self._lists.setdefault(op.key, [])
                for v in op.members:
                    lst.insert(0, v)
                results.append(True)
            elif op.operation_type == _OT.RPUSH:
                self._lists.setdefault(op.key, []).extend(op.members)
                results.append(True)
            elif op.operation_type == _OT.XADD:
                resolved = self._next_stream_id() if op.value == "*" else op.value
                self._streams.setdefault(op.key, []).append((resolved, dict(op.hash_fields)))
                results.append(True)
            else:
                results.append(True)
        return rustycluster_pb2.BatchWriteResponse(success=True, operation_results=results)

    # Stub out bytes operations
    def HSetBytes(self, request, context):
        return rustycluster_pb2.HSetBytesResponse(success=True)

    def HGetBytes(self, request, context):
        return rustycluster_pb2.HGetBytesResponse(value=b"", found=False)

    def HScan(self, request, context):
        resp = rustycluster_pb2.HScanResponse(next_cursor=0)
        resp.fields.update(self._hashes.get(request.key, {}))
        return resp

    def HScanBytes(self, request, context):
        return rustycluster_pb2.HScanBytesResponse(next_cursor=0)

    def HGetAllBytes(self, request, context):
        return rustycluster_pb2.HGetAllBytesResponse()

    def HIncrBy(self, request, context):
        h = self._hashes.setdefault(request.key, {})
        new_val = int(h.get(request.field, "0")) + request.value
        h[request.field] = str(new_val)
        return rustycluster_pb2.HIncrByResponse(new_value=new_val)

    def HDecrBy(self, request, context):
        h = self._hashes.setdefault(request.key, {})
        new_val = int(h.get(request.field, "0")) - request.value
        h[request.field] = str(new_val)
        return rustycluster_pb2.HDecrByResponse(new_value=new_val)

    def HIncrByFloat(self, request, context):
        h = self._hashes.setdefault(request.key, {})
        new_val = float(h.get(request.field, "0")) + request.value
        h[request.field] = str(new_val)
        return rustycluster_pb2.HIncrByFloatResponse(new_value=new_val)


@pytest.fixture(scope="module")
def mock_server():
    """Spin up a local gRPC server for the duration of the test module."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    servicer = MockKeyValueServicer()
    rustycluster_pb2_grpc.add_KeyValueServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("localhost:0")  # OS assigns free port
    server.start()
    yield f"localhost:{port}", servicer
    server.stop(grace=1)


@pytest.fixture
def client(mock_server):
    """Return a RustyClusterClient connected to the mock server."""
    from rustycluster import RustyClusterConfig, RustyClusterSettings, get_client
    target, _ = mock_server
    settings = RustyClusterSettings(
        clusters={
            "test": RustyClusterConfig(
                nodes=target,
                username="admin",
                password="secret",
                max_retries=0,
            ),
        },
    )
    c = get_client("test", settings=settings)
    yield c
    c.close()


class TestIntegrationStringOps:
    def test_set_and_get(self, client):
        assert client.set("integ:key", "hello") is True
        assert client.get("integ:key") == "hello"

    def test_get_missing_returns_none(self, client):
        assert client.get("integ:no_such_key_xyz") is None

    def test_delete(self, client):
        client.set("integ:del", "bye")
        assert client.exists("integ:del") is True
        client.delete("integ:del")
        assert client.get("integ:del") is None

    def test_set_nx(self, client):
        client.delete("integ:nx")
        assert client.set_nx("integ:nx", "first") is True
        assert client.set_nx("integ:nx", "second") is False
        assert client.get("integ:nx") == "first"

    def test_mget(self, client):
        client.set("integ:m1", "v1")
        client.set("integ:m2", "v2")
        result = client.mget("integ:m1", "integ:m2", "integ:missing")
        assert result[0] == "v1"
        assert result[1] == "v2"
        assert result[2] is None


class TestIntegrationNumericOps:
    def test_incr_by(self, client):
        client.set("integ:counter", "0")
        result = client.incr_by("integ:counter", 5)
        assert result == 5

    def test_decr_by(self, client):
        client.set("integ:counter2", "10")
        result = client.decr_by("integ:counter2", 3)
        assert result == 7


class TestIntegrationHashOps:
    def test_hset_hget(self, client):
        client.hset("integ:user:1", "name", "Alice")
        assert client.hget("integ:user:1", "name") == "Alice"

    def test_hmset_hget_all(self, client):
        client.hmset("integ:user:2", {"name": "Bob", "age": "30"})
        data = client.hget_all("integ:user:2")
        assert data.get("name") == "Bob"
        assert data.get("age") == "30"

    def test_hdel(self, client):
        client.hmset("integ:user:3", {"a": "1", "b": "2"})
        deleted = client.hdel("integ:user:3", "a")
        assert deleted == 1
        assert client.hget("integ:user:3", "a") is None

    def test_hlen(self, client):
        client.hmset("integ:hlen", {"f1": "v1", "f2": "v2", "f3": "v3"})
        assert client.hlen("integ:hlen") == 3

    def test_hexists(self, client):
        client.hset("integ:hex", "field1", "val")
        assert client.hexists("integ:hex", "field1") is True
        assert client.hexists("integ:hex", "nonexistent") is False


class TestIntegrationSetOps:
    def test_sadd_smembers(self, client):
        client.sadd("integ:tags", "python", "grpc", "redis")
        members = client.smembers("integ:tags")
        assert set(members) >= {"python", "grpc", "redis"}

    def test_srem(self, client):
        client.sadd("integ:tags2", "a", "b", "c")
        removed = client.srem("integ:tags2", "b")
        assert removed == 1
        assert "b" not in client.smembers("integ:tags2")

    def test_scard(self, client):
        client.sadd("integ:card", "x", "y", "z")
        assert client.scard("integ:card") == 3


class TestIntegrationBatchOps:
    def test_batch_write(self, client):
        from rustycluster.batch import BatchOperationBuilder as BOB
        ops = [
            BOB.set("integ:b1", "val1"),
            BOB.set("integ:b2", "val2"),
            BOB.hset("integ:bh", "field", "fval"),
        ]
        results = client.batch_write(ops)
        assert all(results)
        assert client.get("integ:b1") == "val1"
        assert client.get("integ:b2") == "val2"
        assert client.hget("integ:bh", "field") == "fval"


class TestIntegrationListOps:
    def test_lpush_then_lrange(self, client):
        client.lpush("integ:q", "a", "b", "c")
        # lpush prepends each value in order -> "a","b","c" => head is "c"
        assert client.lrange("integ:q", 0, -1) == ["c", "b", "a"]

    def test_rpush_and_llen(self, client):
        client.rpush("integ:rq", "x", "y", "z")
        assert client.llen("integ:rq") == 3
        assert client.lrange("integ:rq", 0, -1) == ["x", "y", "z"]

    def test_lpop_missing_returns_empty(self, client):
        assert client.lpop("integ:no_such_list_xyz") == []

    def test_lpop_default_single(self, client):
        client.rpush("integ:lp", "1", "2", "3")
        assert client.lpop("integ:lp") == ["1"]

    def test_lpop_with_count(self, client):
        client.rpush("integ:lp2", "a", "b", "c", "d")
        assert client.lpop("integ:lp2", count=2) == ["a", "b"]
        assert client.lrange("integ:lp2", 0, -1) == ["c", "d"]

    def test_rpop_with_count(self, client):
        client.rpush("integ:rp", "1", "2", "3", "4")
        assert client.rpop("integ:rp", count=2) == ["4", "3"]

    def test_lpushx_on_absent_returns_zero(self, client):
        assert client.lpushx("integ:nope_xxx", "x") == 0
        # ensure the list was NOT created
        assert client.llen("integ:nope_xxx") == 0

    def test_lpushx_on_existing(self, client):
        client.rpush("integ:lpx", "init")
        assert client.lpushx("integ:lpx", "head") == 2
        assert client.lrange("integ:lpx", 0, -1) == ["head", "init"]

    def test_rpushx_on_absent_returns_zero(self, client):
        assert client.rpushx("integ:nope_yyy", "x") == 0

    def test_ltrim_reflected_in_lrange(self, client):
        client.rpush("integ:lt", "a", "b", "c", "d", "e")
        assert client.ltrim("integ:lt", 1, 3) is True
        assert client.lrange("integ:lt", 0, -1) == ["b", "c", "d"]

    def test_lindex(self, client):
        client.rpush("integ:li", "x", "y", "z")
        assert client.lindex("integ:li", 0) == "x"
        assert client.lindex("integ:li", -1) == "z"
        assert client.lindex("integ:li", 99) is None

    def test_lset(self, client):
        client.rpush("integ:ls", "a", "b", "c")
        assert client.lset("integ:ls", 1, "B") is True
        assert client.lrange("integ:ls", 0, -1) == ["a", "B", "c"]

    def test_lrem_positive_count(self, client):
        client.rpush("integ:lr", "a", "b", "a", "c", "a")
        assert client.lrem("integ:lr", 2, "a") == 2
        # removes first two "a" from head
        assert client.lrange("integ:lr", 0, -1) == ["b", "c", "a"]

    def test_lrem_negative_count(self, client):
        client.rpush("integ:lr2", "a", "b", "a", "c", "a")
        assert client.lrem("integ:lr2", -2, "a") == 2
        # removes last two "a" from tail
        assert client.lrange("integ:lr2", 0, -1) == ["a", "b", "c"]

    def test_lrem_zero_removes_all(self, client):
        client.rpush("integ:lr3", "a", "b", "a", "c", "a")
        assert client.lrem("integ:lr3", 0, "a") == 3
        assert client.lrange("integ:lr3", 0, -1) == ["b", "c"]

    def test_linsert_before(self, client):
        client.rpush("integ:in", "a", "c")
        assert client.linsert("integ:in", "c", "b", before=True) == 3
        assert client.lrange("integ:in", 0, -1) == ["a", "b", "c"]

    def test_linsert_after(self, client):
        client.rpush("integ:in2", "a", "c")
        assert client.linsert("integ:in2", "a", "b", before=False) == 3
        assert client.lrange("integ:in2", 0, -1) == ["a", "b", "c"]

    def test_linsert_pivot_missing(self, client):
        client.rpush("integ:in3", "a", "b")
        assert client.linsert("integ:in3", "z", "x") == -1

    def test_linsert_absent_key(self, client):
        assert client.linsert("integ:no_in_key", "p", "v") == 0

    def test_lpos_no_match(self, client):
        client.rpush("integ:lpos0", "a", "b", "c")
        assert client.lpos("integ:lpos0", "z") == []

    def test_lpos_first_match(self, client):
        client.rpush("integ:lpos1", "a", "b", "a", "c", "a")
        # default rank=1, count=1 (single match)
        assert client.lpos("integ:lpos1", "a") == [0]

    def test_lpos_with_rank_and_count(self, client):
        client.rpush("integ:lpos2", "a", "b", "a", "c", "a")
        assert client.lpos("integ:lpos2", "a", rank=1, count=0) == [0, 2, 4]
        assert client.lpos("integ:lpos2", "a", rank=-1, count=2) == [4, 2]


class TestIntegrationStreamOps:
    def test_xadd_auto_id_and_xread(self, client):
        eid = client.xadd("integ:s1", {"f1": "v1", "f2": "v2"})
        assert eid != "" and eid != "*"
        entries = client.xread([("integ:s1", "0")])
        assert len(entries) == 1
        key, returned_id, fields = entries[0]
        assert key == "integ:s1"
        assert returned_id == eid
        assert fields == {"f1": "v1", "f2": "v2"}

    def test_xread_after_last_id_returns_empty(self, client):
        eid = client.xadd("integ:s2", {"k": "v"})
        # Ask for entries strictly after the only known id
        assert client.xread([("integ:s2", eid)]) == []

    def test_xread_with_count(self, client):
        client.xadd("integ:s3", {"a": "1"})
        client.xadd("integ:s3", {"a": "2"})
        client.xadd("integ:s3", {"a": "3"})
        entries = client.xread([("integ:s3", "0")], count=2)
        assert len(entries) == 2


class TestIntegrationListStreamBatch:
    def test_batch_write_list_and_stream(self, client):
        from rustycluster.batch import BatchOperationBuilder as BOB
        ops = [
            BOB.lpush("integ:bq", ["a", "b"]),
            BOB.xadd("integ:bs", {"f": "v"}),
        ]
        results = client.batch_write(ops)
        assert all(results)
        assert client.lrange("integ:bq", 0, -1) == ["b", "a"]
        entries = client.xread([("integ:bs", "0")])
        assert len(entries) == 1
        assert entries[0][2] == {"f": "v"}


class TestIntegrationSortedSetOps:
    def test_zadd_new_member(self, client):
        assert client.zadd("integ:z", 1.0, "a") == 1

    def test_zadd_score_update_returns_zero(self, client):
        client.zadd("integ:z2", 1.0, "a")
        assert client.zadd("integ:z2", 2.0, "a") == 0

    def test_zcard(self, client):
        client.zadd("integ:zcard", 1.0, "a")
        client.zadd("integ:zcard", 2.0, "b")
        assert client.zcard("integ:zcard") == 2

    def test_zscore_found(self, client):
        client.zadd("integ:zscore", 3.14, "pi")
        assert client.zscore("integ:zscore", "pi") == pytest.approx(3.14)

    def test_zscore_missing_returns_none(self, client):
        assert client.zscore("integ:zscore_miss", "ghost") is None

    def test_zrange_ordered_by_score(self, client):
        client.zadd("integ:zrange", 3.0, "c")
        client.zadd("integ:zrange", 1.0, "a")
        client.zadd("integ:zrange", 2.0, "b")
        assert client.zrange("integ:zrange", 0, -1) == ["a", "b", "c"]

    def test_zrange_with_scores(self, client):
        client.zadd("integ:zws", 10.0, "x")
        client.zadd("integ:zws", 20.0, "y")
        result = client.zrange("integ:zws", 0, -1, with_scores=True)
        assert result == [("x", 10.0), ("y", 20.0)]

    def test_zrange_by_score(self, client):
        client.zadd("integ:zbs", 1.0, "a")
        client.zadd("integ:zbs", 5.0, "b")
        client.zadd("integ:zbs", 10.0, "c")
        assert client.zrange_by_score("integ:zbs", 1.0, 6.0) == ["a", "b"]

    def test_zrange_by_score_with_scores(self, client):
        client.zadd("integ:zbs2", 2.0, "a")
        client.zadd("integ:zbs2", 4.0, "b")
        result = client.zrange_by_score("integ:zbs2", 0, 10, with_scores=True)
        assert result == [("a", 2.0), ("b", 4.0)]

    def test_zrange_by_score_with_limit(self, client):
        client.zadd("integ:zlim", 1.0, "a")
        client.zadd("integ:zlim", 2.0, "b")
        client.zadd("integ:zlim", 3.0, "c")
        result = client.zrange_by_score("integ:zlim", 0, 10, offset=1, count=2)
        assert result == ["b", "c"]

    def test_zrem(self, client):
        client.zadd("integ:zrem", 1.0, "a")
        client.zadd("integ:zrem", 2.0, "b")
        assert client.zrem("integ:zrem", "a", "b") == 2
        assert client.zcard("integ:zrem") == 0

    def test_zrem_nonexistent_member_ignored(self, client):
        client.zadd("integ:zrem2", 1.0, "a")
        assert client.zrem("integ:zrem2", "a", "ghost") == 1


class TestIntegrationBlockingPops:
    def test_blpop_returns_key_and_value(self, client):
        client.rpush("integ:blq", "first", "second")
        result = client.blpop("integ:blq", timeout=1.0)
        assert result == ("integ:blq", "first")

    def test_blpop_empty_list_returns_none(self, client):
        assert client.blpop("integ:blq_empty_xyz", timeout=0.1) is None

    def test_blpop_checks_keys_in_order(self, client):
        client.rpush("integ:blq2", "val")
        result = client.blpop("integ:blq_none_xyz", "integ:blq2", timeout=1.0)
        assert result == ("integ:blq2", "val")

    def test_brpop_returns_key_and_value(self, client):
        client.rpush("integ:brq", "first", "last")
        result = client.brpop("integ:brq", timeout=1.0)
        assert result == ("integ:brq", "last")

    def test_brpop_empty_list_returns_none(self, client):
        assert client.brpop("integ:brq_empty_xyz", timeout=0.1) is None


class TestIntegrationSystem:
    def test_ping(self, client):
        assert client.ping() is True

    def test_health_check(self, client):
        assert client.health_check() is True


def _start_mock_server() -> tuple[grpc.Server, str]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    servicer = MockKeyValueServicer()
    rustycluster_pb2_grpc.add_KeyValueServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("localhost:0")
    server.start()
    return server, f"localhost:{port}"


class TestIntegrationFailover:
    def test_fails_over_when_primary_is_down(self, caplog):
        from rustycluster import (
            RustyClusterConfig,
            RustyClusterSettings,
            get_client,
        )

        server_a, target_a = _start_mock_server()
        server_b, target_b = _start_mock_server()

        try:
            settings = RustyClusterSettings(
                clusters={
                    "failover": RustyClusterConfig(
                        nodes=f"{target_a},{target_b}",
                        username="admin",
                        password="secret",
                        max_retries=1,
                        retry_backoff_base=0.01,
                        retry_backoff_max=0.1,
                        timeout_seconds=2.0,
                    ),
                },
            )
            c = get_client("failover", settings=settings)
            try:
                assert c.set("failover:k", "v1") is True
                assert c.get("failover:k") == "v1"

                # Kill the primary
                server_a.stop(grace=0)

                import logging as _logging
                with caplog.at_level(_logging.WARNING, logger="rustycluster.failover"):
                    assert c.set("failover:k", "v2") is True
                    assert c.get("failover:k") == "v2"

                assert any(
                    "Failing over" in r.message and target_a in r.message and target_b in r.message
                    for r in caplog.records
                ), f"expected failover log, got: {[r.message for r in caplog.records]}"
            finally:
                c.close()
        finally:
            server_a.stop(grace=0)
            server_b.stop(grace=0)

    def test_raises_when_all_nodes_exhausted(self):
        from rustycluster import (
            RustyClusterConfig,
            RustyClusterSettings,
            get_client,
        )
        from rustycluster.exceptions import ConnectionError as RCConnectionError

        server_a, target_a = _start_mock_server()
        server_b, target_b = _start_mock_server()

        settings = RustyClusterSettings(
            clusters={
                "exhaust": RustyClusterConfig(
                    nodes=f"{target_a},{target_b}",
                    username="admin",
                    password="secret",
                    max_retries=1,
                    retry_backoff_base=0.01,
                    retry_backoff_max=0.1,
                    timeout_seconds=2.0,
                ),
            },
        )
        c = get_client("exhaust", settings=settings)
        try:
            server_a.stop(grace=0)
            server_b.stop(grace=0)
            with pytest.raises(RCConnectionError):
                c.set("failover:k", "v")
        finally:
            c.close()
