"""
Integration test template using a mock gRPC server.

These tests spin up a local gRPC server that implements KeyValueServiceServicer,
so they test the full client-server round-trip without a real RustyCluster instance.

Run with:
    pytest tests/integration/ -v
"""

from __future__ import annotations

import threading
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

    def BatchWrite(self, request, context):
        results = []
        for op in request.operations:
            _OT = rustycluster_pb2.BatchOperation.OperationType
            if op.operation_type == _OT.SET:
                self._store[op.key] = op.value
                results.append(True)
            elif op.operation_type == _OT.DELETE:
                self._store.pop(op.key, None)
                results.append(True)
            elif op.operation_type == _OT.HSET:
                self._hashes.setdefault(op.key, {})[op.field] = op.value
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
    from rustycluster import RustyClusterConfig, get_client
    target, _ = mock_server
    host, port = target.split(":")
    config = RustyClusterConfig(
        host=host,
        port=int(port),
        username="admin",
        password="secret",
        max_retries=0,
    )
    c = get_client(config)
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


class TestIntegrationSystem:
    def test_ping(self, client):
        assert client.ping() is True

    def test_health_check(self, client):
        assert client.health_check() is True
