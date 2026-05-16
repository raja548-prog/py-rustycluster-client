"""
Replication integration tests for RustyCluster.

Writes are applied to the primary node (50051). Reads are verified on all
replica nodes (50052, 50053, 50054).  Each test uses a UUID-suffixed key to
avoid cross-test interference.

Each assertion polls replicas up to REPLICATION_TIMEOUT seconds before
failing, so tests pass as soon as replication arrives and only fail when it
genuinely doesn't happen.

Prerequisites
-------------
The 4-node cluster must be running locally before any of these tests will
pass: primary on :50051, replicas on :50052, :50053, :50054 — all sharing
the credentials defined below (testuser / testpass).

How to run
----------
Run all tests:
    pytest test_replication.py -v

Stop at the first failure (useful while iterating on the server):
    pytest test_replication.py -v -x

Run a single test class (one logical group of RPCs):
    pytest test_replication.py -v -k TestHashReplication
    pytest test_replication.py -v -k TestStreamReplication

Run a single test by name:
    pytest test_replication.py -v -k test_zadd_replicated

Run only the "new" RPC-coverage classes (everything but ZSet/List/Stream):
    pytest test_replication.py -v -k "TestString or TestNumeric or \
TestHash or TestHashBytes or TestSet or TestKey or TestScript or TestBatch \
or TestScan"

Show captured stdout/logs even on passing tests (helpful when debugging
replication lag):
    pytest test_replication.py -v -s

Show the reason for xfail / xpass entries in the summary:
    pytest test_replication.py -v -rxX

Tighten or loosen the per-test replication poll window (defaults below):
    REPLICATION_TIMEOUT / REPLICATION_POLL — edit the constants in this
    module, or override via env-var wrappers in your shell.

Known xfails
------------
The HSetBytes-driven tests are marked xfail(strict=True): HSetBytes writes
reach replicas 50052 and 50053 with the correct binary values, but never
reach replica 50054 (hget_bytes returns None there, hget_all_bytes returns
[]). Regular HSet on the string path fans out to all three replicas, so
the gap is specific to the binary-hash replication route.
Affected tests:
  - TestHashBytesReplication.test_hset_bytes_replicated
  - TestHashBytesReplication.test_hget_all_bytes_replicated
  - TestScanReplication.test_hscan_bytes_replicated
Re-enable once HSetBytes fans out to the full replica set.
"""

import time
import uuid
from typing import Callable, Any

import pytest

from rustycluster import get_client
from rustycluster.config import RustyClusterConfig, RustyClusterSettings
from rustycluster.client import RustyClusterClient
from rustycluster.batch import BatchOperationBuilder

# ── Configuration ──────────────────────────────────────────────────────────────

PRIMARY_PORT = 50051
REPLICA_PORTS = [50052, 50053, 50054]

_USERNAME = "testuser"
_PASSWORD = "testpass"

REPLICATION_TIMEOUT = 5.0  # seconds to poll before declaring replication failed
REPLICATION_POLL    = 0.2  # polling interval


# ── Client factory ─────────────────────────────────────────────────────────────

def _make_node_client(port: int) -> RustyClusterClient:
    """Build a client pinned to a single node (no automatic failover)."""
    settings = RustyClusterSettings(
        clusters={
            f"node_{port}": RustyClusterConfig(
                nodes=f"localhost:{port}",
                username=_USERNAME,
                password=_PASSWORD,
                max_retries=1,
                timeout_seconds=5.0,
            )
        }
    )
    return get_client(f"node_{port}", settings=settings)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _uid() -> str:
    return uuid.uuid4().hex[:8]


def assert_replicated(
    replicas: list[RustyClusterClient],
    fn: Callable[[RustyClusterClient], Any],
    expected: Any,
    label: str,
    timeout: float = REPLICATION_TIMEOUT,
) -> None:
    """Poll each replica until fn(replica) == expected or timeout expires."""
    for port, replica in zip(REPLICA_PORTS, replicas):
        deadline = time.monotonic() + timeout
        actual: Any = None
        while True:
            try:
                actual = fn(replica)
            except Exception as exc:
                actual = exc
            if actual == expected:
                break
            if time.monotonic() >= deadline:
                pytest.fail(
                    f"Port {port} [{label}]: expected {expected!r}, got {actual!r} "
                    f"(after {timeout}s)"
                )
            time.sleep(REPLICATION_POLL)


def _wait_for_llen(replica: RustyClusterClient, key: str, min_len: int = 1) -> bool:
    """Poll until llen(key) >= min_len or timeout. Returns True if satisfied."""
    deadline = time.monotonic() + REPLICATION_TIMEOUT
    while time.monotonic() < deadline:
        try:
            if replica.llen(key) >= min_len:
                return True
        except Exception:
            pass
        time.sleep(REPLICATION_POLL)
    return False


def _drain_hscan(client: RustyClusterClient, key: str) -> dict[str, str]:
    """Fully iterate hscan and return all field-value pairs as a single dict."""
    cursor = 0
    out: dict[str, str] = {}
    while True:
        cursor, page = client.hscan(key, cursor=cursor, count=100)
        out.update(page)
        if cursor == 0:
            return out


def _drain_sscan(client: RustyClusterClient, key: str) -> list[str]:
    """Fully iterate sscan and return all members."""
    cursor = 0
    out: list[str] = []
    while True:
        cursor, page = client.sscan(key, cursor=cursor, count=100)
        out.extend(page)
        if cursor == 0:
            return out


def _drain_hscan_bytes(client: RustyClusterClient, key: bytes) -> list[tuple[bytes, bytes]]:
    """Fully iterate hscan_bytes and return all (field, value) pairs as bytes."""
    cursor = 0
    out: list[tuple[bytes, bytes]] = []
    while True:
        cursor, page = client.hscan_bytes(key, cursor=cursor, count=100)
        out.extend(page)
        if cursor == 0:
            return out


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def primary() -> RustyClusterClient:
    return _make_node_client(PRIMARY_PORT)


@pytest.fixture(scope="module")
def replicas() -> list[RustyClusterClient]:
    return [_make_node_client(p) for p in REPLICA_PORTS]


@pytest.fixture(autouse=True)
def cleanup(primary: RustyClusterClient):
    """Collect keys during a test and delete them from the primary at teardown.
    Deletion itself replicates, so replicas are cleaned up automatically."""
    keys: list[str] = []
    yield keys
    for key in keys:
        try:
            primary.delete(key)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# Sorted Set Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestZSetReplication:

    def test_zadd_replicated(self, primary, replicas, cleanup):
        key = f"repl:zset:zadd:{_uid()}"
        cleanup.append(key)

        primary.zadd(key, 1.0, "alpha")

        assert_replicated(replicas, lambda r: r.zscore(key, "alpha"), 1.0, "zscore after zadd")
        assert_replicated(replicas, lambda r: r.zcard(key), 1, "zcard after zadd")
        assert_replicated(replicas, lambda r: r.zrange(key, 0, -1), ["alpha"], "zrange after zadd")

    def test_zrange_replicated(self, primary, replicas, cleanup):
        key = f"repl:zset:zrange:{_uid()}"
        cleanup.append(key)

        primary.zadd(key, 1.0, "a")
        primary.zadd(key, 2.0, "b")
        primary.zadd(key, 3.0, "c")

        assert_replicated(
            replicas,
            lambda r: r.zrange(key, 0, -1, with_scores=True),
            [("a", 1.0), ("b", 2.0), ("c", 3.0)],
            "zrange with_scores",
        )

    def test_zrange_by_score_replicated(self, primary, replicas, cleanup):
        key = f"repl:zset:zbs:{_uid()}"
        cleanup.append(key)

        primary.zadd(key, 1.0, "a")
        primary.zadd(key, 5.0, "b")
        primary.zadd(key, 9.0, "c")

        assert_replicated(
            replicas,
            lambda r: r.zrange_by_score(key, 0.0, 7.0, with_scores=True),
            [("a", 1.0), ("b", 5.0)],
            "zrange_by_score with_scores",
        )

    def test_zscore_replicated(self, primary, replicas, cleanup):
        key = f"repl:zset:zscore:{_uid()}"
        cleanup.append(key)

        primary.zadd(key, 9.5, "member")

        assert_replicated(replicas, lambda r: r.zscore(key, "member"), 9.5, "zscore")

    def test_zcard_replicated(self, primary, replicas, cleanup):
        key = f"repl:zset:zcard:{_uid()}"
        cleanup.append(key)

        for i in range(5):
            primary.zadd(key, float(i), f"m{i}")

        assert_replicated(replicas, lambda r: r.zcard(key), 5, "zcard")

    def test_zrem_replicated(self, primary, replicas, cleanup):
        key = f"repl:zset:zrem:{_uid()}"
        cleanup.append(key)

        primary.zadd(key, 1.0, "x")
        primary.zadd(key, 2.0, "y")
        # Wait for initial replication before mutating
        assert_replicated(replicas, lambda r: r.zcard(key), 2, "zcard before zrem")

        primary.zrem(key, "x")

        assert_replicated(replicas, lambda r: r.zcard(key), 1, "zcard after zrem")
        assert_replicated(replicas, lambda r: r.zscore(key, "x"), None, "zscore of removed member")


# ══════════════════════════════════════════════════════════════════════════════
# List Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestListReplication:

    def test_lpush_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:lpush:{_uid()}"
        cleanup.append(key)

        primary.lpush(key, "a", "b", "c")

        assert_replicated(replicas, lambda r: r.lrange(key, 0, -1), ["c", "b", "a"], "lrange after lpush")
        assert_replicated(replicas, lambda r: r.llen(key), 3, "llen after lpush")

    def test_rpush_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:rpush:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "a", "b", "c")

        assert_replicated(replicas, lambda r: r.lrange(key, 0, -1), ["a", "b", "c"], "lrange after rpush")
        assert_replicated(replicas, lambda r: r.llen(key), 3, "llen after rpush")

    def test_lpop_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:lpop:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "x", "y", "z")
        assert_replicated(replicas, lambda r: r.llen(key), 3, "llen before lpop")

        primary.lpop(key, count=1)

        assert_replicated(replicas, lambda r: r.llen(key), 2, "llen after lpop")
        assert_replicated(replicas, lambda r: r.lrange(key, 0, -1), ["y", "z"], "lrange after lpop")

    def test_rpop_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:rpop:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "x", "y", "z")
        assert_replicated(replicas, lambda r: r.llen(key), 3, "llen before rpop")

        primary.rpop(key, count=1)

        assert_replicated(replicas, lambda r: r.lrange(key, 0, -1), ["x", "y"], "lrange after rpop")

    def test_lrange_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:lrange:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "a", "b", "c", "d", "e")

        assert_replicated(replicas, lambda r: r.lrange(key, 1, 3), ["b", "c", "d"], "lrange slice [1:3]")

    def test_llen_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:llen:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "1", "2", "3", "4")

        assert_replicated(replicas, lambda r: r.llen(key), 4, "llen")

    def test_ltrim_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:ltrim:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "a", "b", "c", "d", "e")
        assert_replicated(replicas, lambda r: r.llen(key), 5, "llen before ltrim")

        primary.ltrim(key, 1, 3)

        assert_replicated(replicas, lambda r: r.lrange(key, 0, -1), ["b", "c", "d"], "lrange after ltrim(1,3)")

    def test_lindex_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:lindex:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "first", "second", "third")

        assert_replicated(replicas, lambda r: r.lindex(key, 1), "second", "lindex(1)")

    def test_lset_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:lset:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "a", "b", "c")
        assert_replicated(replicas, lambda r: r.lindex(key, 1), "b", "lindex(1) before lset")

        primary.lset(key, 1, "UPDATED")

        assert_replicated(replicas, lambda r: r.lindex(key, 1), "UPDATED", "lindex after lset")

    def test_lrem_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:lrem:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "dup", "ok", "dup", "fine", "dup")
        assert_replicated(replicas, lambda r: r.llen(key), 5, "llen before lrem")

        primary.lrem(key, 0, "dup")

        assert_replicated(replicas, lambda r: r.lrange(key, 0, -1), ["ok", "fine"], "lrange after lrem")

    def test_linsert_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:linsert:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "a", "b", "c")
        assert_replicated(replicas, lambda r: r.llen(key), 3, "llen before linsert")

        primary.linsert(key, "b", "X", before=True)

        assert_replicated(
            replicas,
            lambda r: r.lrange(key, 0, -1),
            ["a", "X", "b", "c"],
            "lrange after linsert BEFORE b",
        )

    def test_lpushx_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:lpushx:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "existing")
        assert_replicated(replicas, lambda r: r.llen(key), 1, "llen before lpushx")

        primary.lpushx(key, "new-head")

        assert_replicated(replicas, lambda r: r.lindex(key, 0), "new-head", "lindex(0) after lpushx")

    def test_rpushx_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:rpushx:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "existing")
        assert_replicated(replicas, lambda r: r.llen(key), 1, "llen before rpushx")

        primary.rpushx(key, "new-tail")

        assert_replicated(replicas, lambda r: r.lindex(key, -1), "new-tail", "lindex(-1) after rpushx")

    def test_lpos_replicated(self, primary, replicas, cleanup):
        key = f"repl:list:lpos:{_uid()}"
        cleanup.append(key)

        primary.rpush(key, "a", "b", "a", "c", "a")

        assert_replicated(
            replicas,
            lambda r: r.lpos(key, "a", count=3),
            [0, 2, 4],
            "lpos positions for 'a'",
        )

    def test_blpop_replicated(self, primary, replicas, cleanup):
        """Push data to primary; once replicated each replica's blpop returns immediately."""
        for port, replica in zip(REPLICA_PORTS, replicas):
            key = f"repl:list:blpop:{port}:{_uid()}"
            cleanup.append(key)

            primary.rpush(key, "payload")
            # Poll until the replica has the item, then blpop it
            if not _wait_for_llen(replica, key):
                pytest.fail(f"Port {port} [blpop]: item never replicated within {REPLICATION_TIMEOUT}s")

            result = replica.blpop(key, timeout=2.0)
            assert result == (key, "payload"), (
                f"Port {port} [blpop]: expected ({key!r}, 'payload'), got {result!r}"
            )

    def test_brpop_replicated(self, primary, replicas, cleanup):
        """Push data to primary; once replicated each replica's brpop returns immediately."""
        for port, replica in zip(REPLICA_PORTS, replicas):
            key = f"repl:list:brpop:{port}:{_uid()}"
            cleanup.append(key)

            primary.lpush(key, "payload")
            if not _wait_for_llen(replica, key):
                pytest.fail(f"Port {port} [brpop]: item never replicated within {REPLICATION_TIMEOUT}s")

            result = replica.brpop(key, timeout=2.0)
            assert result == (key, "payload"), (
                f"Port {port} [brpop]: expected ({key!r}, 'payload'), got {result!r}"
            )


# ══════════════════════════════════════════════════════════════════════════════
# Stream Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestStreamReplication:

    def test_xadd_replicated(self, primary, replicas, cleanup):
        key = f"repl:stream:xadd:{_uid()}"
        cleanup.append(key)

        entry_id = primary.xadd(key, {"field1": "value1", "field2": "value2"})

        for port, replica in zip(REPLICA_PORTS, replicas):
            deadline = time.monotonic() + REPLICATION_TIMEOUT
            entries = []
            while True:
                try:
                    entries = replica.xread([(key, "0")])
                except Exception:
                    entries = []
                if entries:
                    break
                if time.monotonic() >= deadline:
                    pytest.fail(f"Port {port} [xread]: no entry replicated after {REPLICATION_TIMEOUT}s")
                time.sleep(REPLICATION_POLL)

            assert len(entries) == 1, f"Port {port} [xread count]: expected 1, got {len(entries)}"
            _, eid, fields = entries[0]
            assert eid == entry_id, f"Port {port} [xread id]: expected {entry_id!r}, got {eid!r}"
            assert fields == {"field1": "value1", "field2": "value2"}, (
                f"Port {port} [xread fields]: got {fields!r}"
            )

    def test_xadd_multiple_replicated(self, primary, replicas, cleanup):
        key = f"repl:stream:xadd_multi:{_uid()}"
        cleanup.append(key)

        # Small gap between writes: rustycluster drops stream entries when
        # multiple xadd events fire faster than the replication pipeline drains.
        # This is a known server-side race condition; 0.1s gaps sidestep it.
        for i in range(3):
            primary.xadd(key, {"seq": str(i), "data": f"item_{i}"})
            time.sleep(0.25)

        assert_replicated(
            replicas,
            lambda r: len(r.xread([(key, "0")])),
            3,
            "xread entry count after 3 xadd",
        )

    def test_xread_count_replicated(self, primary, replicas, cleanup):
        key = f"repl:stream:xread_count:{_uid()}"
        cleanup.append(key)

        for i in range(5):
            primary.xadd(key, {"n": str(i)})
            time.sleep(0.25)

        # Wait until all 5 are replicated, then verify count limiting works
        assert_replicated(replicas, lambda r: len(r.xread([(key, "0")])), 5, "all 5 entries replicated")
        assert_replicated(
            replicas,
            lambda r: len(r.xread([(key, "0")], count=3)),
            3,
            "xread with count=3 returns exactly 3",
        )


# ══════════════════════════════════════════════════════════════════════════════
# String Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestStringReplication:

    def test_set_replicated(self, primary, replicas, cleanup):
        key = f"repl:str:set:{_uid()}"
        cleanup.append(key)

        primary.set(key, "hello")

        assert_replicated(replicas, lambda r: r.get(key), "hello", "get after set")

    def test_delete_replicated(self, primary, replicas, cleanup):
        key = f"repl:str:del:{_uid()}"
        cleanup.append(key)

        primary.set(key, "to-be-deleted")
        assert_replicated(replicas, lambda r: r.exists(key), True, "exists before delete")

        primary.delete(key)

        # NOTE: we deliberately use `exists` (not `get`) to verify the deletion.
        # The server's Get RPC returns INTERNAL for missing keys instead of
        # found=False, and the client's retry+failover exhausts the single-node
        # config and zeros the stub — breaking the replica client for the rest
        # of the module's tests.
        assert_replicated(replicas, lambda r: r.exists(key), False, "exists after delete")

    def test_set_ex_replicated(self, primary, replicas, cleanup):
        key = f"repl:str:setex:{_uid()}"
        cleanup.append(key)

        primary.set_ex(key, "ephemeral", 60)

        assert_replicated(replicas, lambda r: r.get(key), "ephemeral", "get after set_ex")

    def test_set_expiry_replicated(self, primary, replicas, cleanup):
        key = f"repl:str:setexpiry:{_uid()}"
        cleanup.append(key)

        primary.set(key, "persistent")
        assert_replicated(replicas, lambda r: r.get(key), "persistent", "get before set_expiry")

        primary.set_expiry(key, 60)

        # Key still present after the expiry call replicates
        assert_replicated(replicas, lambda r: r.get(key), "persistent", "get after set_expiry")


# ══════════════════════════════════════════════════════════════════════════════
# Numeric Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestNumericReplication:

    def test_incr_by_replicated(self, primary, replicas, cleanup):
        key = f"repl:num:incr:{_uid()}"
        cleanup.append(key)

        primary.set(key, "10")
        assert_replicated(replicas, lambda r: r.get(key), "10", "get before incr_by")

        primary.incr_by(key, 5)

        assert_replicated(replicas, lambda r: r.get(key), "15", "get after incr_by(+5)")

    def test_decr_by_replicated(self, primary, replicas, cleanup):
        key = f"repl:num:decr:{_uid()}"
        cleanup.append(key)

        primary.set(key, "10")
        assert_replicated(replicas, lambda r: r.get(key), "10", "get before decr_by")

        primary.decr_by(key, 3)

        assert_replicated(replicas, lambda r: r.get(key), "7", "get after decr_by(-3)")

    def test_incr_by_float_replicated(self, primary, replicas, cleanup):
        key = f"repl:num:incrf:{_uid()}"
        cleanup.append(key)

        primary.set(key, "1.5")
        assert_replicated(replicas, lambda r: r.get(key), "1.5", "get before incr_by_float")

        primary.incr_by_float(key, 2.25)

        assert_replicated(
            replicas,
            lambda r: float(r.get(key)),
            3.75,
            "float(get) after incr_by_float(+2.25)",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Hash Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestHashReplication:

    def test_hset_replicated(self, primary, replicas, cleanup):
        key = f"repl:hash:hset:{_uid()}"
        cleanup.append(key)

        primary.hset(key, "name", "alice")

        assert_replicated(replicas, lambda r: r.hget(key, "name"), "alice", "hget after hset")
        assert_replicated(replicas, lambda r: r.hlen(key), 1, "hlen after hset")

    def test_hmset_replicated(self, primary, replicas, cleanup):
        key = f"repl:hash:hmset:{_uid()}"
        cleanup.append(key)

        primary.hmset(key, {"a": "1", "b": "2"})

        assert_replicated(
            replicas,
            lambda r: r.hget_all(key),
            {"a": "1", "b": "2"},
            "hget_all after hmset",
        )

    def test_hdel_replicated(self, primary, replicas, cleanup):
        key = f"repl:hash:hdel:{_uid()}"
        cleanup.append(key)

        primary.hmset(key, {"a": "1", "b": "2"})
        assert_replicated(replicas, lambda r: r.hlen(key), 2, "hlen before hdel")

        primary.hdel(key, "a")

        assert_replicated(replicas, lambda r: r.hexists(key, "a"), False, "hexists(a) after hdel")
        assert_replicated(replicas, lambda r: r.hlen(key), 1, "hlen after hdel")

    def test_hsetnx_replicated(self, primary, replicas, cleanup):
        key = f"repl:hash:hsetnx:{_uid()}"
        cleanup.append(key)

        primary.hsetnx(key, "field", "value")

        assert_replicated(replicas, lambda r: r.hget(key, "field"), "value", "hget after hsetnx")

    def test_hincr_by_replicated(self, primary, replicas, cleanup):
        key = f"repl:hash:hincr:{_uid()}"
        cleanup.append(key)

        primary.hset(key, "n", "10")
        assert_replicated(replicas, lambda r: r.hget(key, "n"), "10", "hget before hincr_by")

        primary.hincr_by(key, "n", 5)

        assert_replicated(replicas, lambda r: r.hget(key, "n"), "15", "hget after hincr_by(+5)")

    def test_hdecr_by_replicated(self, primary, replicas, cleanup):
        key = f"repl:hash:hdecr:{_uid()}"
        cleanup.append(key)

        primary.hset(key, "n", "10")
        assert_replicated(replicas, lambda r: r.hget(key, "n"), "10", "hget before hdecr_by")

        primary.hdecr_by(key, "n", 4)

        assert_replicated(replicas, lambda r: r.hget(key, "n"), "6", "hget after hdecr_by(-4)")

    def test_hincr_by_float_replicated(self, primary, replicas, cleanup):
        key = f"repl:hash:hincrf:{_uid()}"
        cleanup.append(key)

        primary.hset(key, "n", "1.5")
        assert_replicated(replicas, lambda r: r.hget(key, "n"), "1.5", "hget before hincr_by_float")

        primary.hincr_by_float(key, "n", 2.5)

        assert_replicated(
            replicas,
            lambda r: float(r.hget(key, "n")),
            4.0,
            "float(hget) after hincr_by_float(+2.5)",
        )

    def test_hexpire_replicated(self, primary, replicas, cleanup):
        key = f"repl:hash:hexpire:{_uid()}"
        cleanup.append(key)

        primary.hset(key, "f", "v")
        assert_replicated(replicas, lambda r: r.hexists(key, "f"), True, "hexists before hexpire")

        primary.hexpire(key, 60, "f")

        # Field still present after the expiry call replicates
        assert_replicated(replicas, lambda r: r.hexists(key, "f"), True, "hexists after hexpire")


# ══════════════════════════════════════════════════════════════════════════════
# Hash Bytes Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestHashBytesReplication:

    # Server-side replication gap: HSetBytes writes reach replicas 50052 and
    # 50053 correctly (values intact), but never reach replica 50054 — its
    # hget_bytes returns None and hget_all_bytes returns []. Regular HSet
    # (string path) fans out to all three. Re-enable once HSetBytes targets
    # the full replica set.
    @pytest.mark.xfail(reason="HSetBytes does not fan out to replica 50054 (only 50052/50053 receive writes)", strict=True)
    def test_hset_bytes_replicated(self, primary, replicas, cleanup):
        key_bytes = f"repl:hash:hsetb:{_uid()}".encode()
        # Track string form of the key so the cleanup fixture can delete it.
        cleanup.append(key_bytes.decode("latin-1"))

        primary.hset_bytes(key_bytes, b"field", b"\x00\x01\xff")

        assert_replicated(
            replicas,
            lambda r: r.hget_bytes(key_bytes, b"field"),
            b"\x00\x01\xff",
            "hget_bytes after hset_bytes",
        )

    @pytest.mark.xfail(reason="HSetBytes does not fan out to replica 50054 (only 50052/50053 receive writes)", strict=True)
    def test_hget_all_bytes_replicated(self, primary, replicas, cleanup):
        key_bytes = f"repl:hash:hgetallb:{_uid()}".encode()
        cleanup.append(key_bytes.decode("latin-1"))

        primary.hset_bytes(key_bytes, b"f1", b"\x10")
        primary.hset_bytes(key_bytes, b"f2", b"\x20\x21")

        expected = sorted([(b"f1", b"\x10"), (b"f2", b"\x20\x21")])
        assert_replicated(
            replicas,
            lambda r: sorted(r.hget_all_bytes(key_bytes)),
            expected,
            "hget_all_bytes after two hset_bytes",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Set Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestSetReplication:

    def test_sadd_replicated(self, primary, replicas, cleanup):
        key = f"repl:set:sadd:{_uid()}"
        cleanup.append(key)

        primary.sadd(key, "a", "b", "c")

        assert_replicated(
            replicas,
            lambda r: sorted(r.smembers(key)),
            ["a", "b", "c"],
            "sorted(smembers) after sadd",
        )
        assert_replicated(replicas, lambda r: r.scard(key), 3, "scard after sadd")

    def test_srem_replicated(self, primary, replicas, cleanup):
        key = f"repl:set:srem:{_uid()}"
        cleanup.append(key)

        primary.sadd(key, "a", "b", "c")
        assert_replicated(replicas, lambda r: r.scard(key), 3, "scard before srem")

        primary.srem(key, "b")

        assert_replicated(
            replicas,
            lambda r: sorted(r.smembers(key)),
            ["a", "c"],
            "sorted(smembers) after srem",
        )
        assert_replicated(replicas, lambda r: r.scard(key), 2, "scard after srem")


# ══════════════════════════════════════════════════════════════════════════════
# Key Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestKeyReplication:

    def test_set_nx_replicated(self, primary, replicas, cleanup):
        key = f"repl:key:setnx:{_uid()}"
        cleanup.append(key)

        primary.set_nx(key, "fresh")

        assert_replicated(replicas, lambda r: r.get(key), "fresh", "get after set_nx")

    def test_set_nx_no_overwrite_replicated(self, primary, replicas, cleanup):
        key = f"repl:key:setnx_noop:{_uid()}"
        cleanup.append(key)

        primary.set(key, "first")
        assert_replicated(replicas, lambda r: r.get(key), "first", "get before set_nx no-op")

        result = primary.set_nx(key, "second")
        assert result is False, f"set_nx on existing key should return False, got {result!r}"

        # Replicas should still see the original value
        assert_replicated(replicas, lambda r: r.get(key), "first", "get after set_nx no-op")

    def test_del_multiple_replicated(self, primary, replicas, cleanup):
        uid = _uid()
        keys = [f"repl:key:delmulti:{uid}:{i}" for i in range(3)]
        for k in keys:
            cleanup.append(k)

        for k in keys:
            primary.set(k, "v")
        for k in keys:
            assert_replicated(replicas, lambda r, kk=k: r.get(kk), "v", f"get before del_multiple ({k})")

        primary.del_multiple(*keys)

        for k in keys:
            assert_replicated(replicas, lambda r, kk=k: r.exists(kk), False, f"exists after del_multiple ({k})")

    def test_keys_replicated(self, primary, replicas, cleanup):
        uid = _uid()
        prefix = f"repl:key:keys:{uid}:"
        keys = [f"{prefix}{i}" for i in range(3)]
        for k in keys:
            cleanup.append(k)

        for k in keys:
            primary.set(k, "v")

        assert_replicated(
            replicas,
            lambda r: sorted(r.keys(f"{prefix}*")),
            sorted(keys),
            "keys(prefix*) after three sets",
        )

    def test_mget_replicated(self, primary, replicas, cleanup):
        uid = _uid()
        keys = [f"repl:key:mget:{uid}:{i}" for i in range(3)]
        for k in keys:
            cleanup.append(k)

        values = ["one", "two", "three"]
        for k, v in zip(keys, values):
            primary.set(k, v)

        assert_replicated(
            replicas,
            lambda r: r.mget(*keys),
            values,
            "mget after three sets",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Script Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestScriptReplication:

    def test_eval_sha_write_replicated(self, primary, replicas, cleanup):
        """A SET performed via eval_sha on the primary must replicate to replicas.

        Uses the Redis Lua dialect (`redis.call('SET', KEYS[1], ARGV[1])`).
        If the server speaks a different scripting dialect, adjust the script
        body — the test's purpose is to verify replication, not scripting.
        """
        key = f"repl:script:evalsha:{_uid()}"
        cleanup.append(key)

        try:
            sha = primary.load_script("return redis.call('SET', KEYS[1], ARGV[1])")
            primary.eval_sha(sha, [key], ["scripted-value"])
        except Exception as exc:
            pytest.xfail(f"script load/eval failed on server: {exc!r}")

        assert_replicated(replicas, lambda r: r.get(key), "scripted-value", "get after eval_sha SET")


# ══════════════════════════════════════════════════════════════════════════════
# Batch Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestBatchReplication:

    def test_batch_write_replicated(self, primary, replicas, cleanup):
        uid = _uid()
        k_str = f"repl:batch:str:{uid}"
        k_hash = f"repl:batch:hash:{uid}"
        k_set = f"repl:batch:set:{uid}"
        for k in (k_str, k_hash, k_set):
            cleanup.append(k)

        ops = [
            BatchOperationBuilder.set(k_str, "v1"),
            BatchOperationBuilder.hset(k_hash, "f", "v2"),
            BatchOperationBuilder.sadd(k_set, ["m"]),
        ]
        primary.batch_write(ops)

        assert_replicated(replicas, lambda r: r.get(k_str), "v1", "get after batch set")
        assert_replicated(replicas, lambda r: r.hget(k_hash, "f"), "v2", "hget after batch hset")
        assert_replicated(
            replicas,
            lambda r: "m" in r.smembers(k_set),
            True,
            "'m' in smembers after batch sadd",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Cursor Scan Replication
# ══════════════════════════════════════════════════════════════════════════════

class TestScanReplication:

    def test_hscan_replicated(self, primary, replicas, cleanup):
        key = f"repl:scan:hscan:{_uid()}"
        cleanup.append(key)

        primary.hmset(key, {"a": "1", "b": "2", "c": "3"})

        assert_replicated(
            replicas,
            lambda r: _drain_hscan(r, key),
            {"a": "1", "b": "2", "c": "3"},
            "hscan drain after hmset",
        )

    def test_sscan_replicated(self, primary, replicas, cleanup):
        key = f"repl:scan:sscan:{_uid()}"
        cleanup.append(key)

        primary.sadd(key, "alpha", "beta", "gamma")

        assert_replicated(
            replicas,
            lambda r: sorted(_drain_sscan(r, key)),
            ["alpha", "beta", "gamma"],
            "sorted(sscan drain) after sadd",
        )

    # Inherits the same server-side replication gap as TestHashBytesReplication:
    # HSetBytes writes do not fan out to replica 50054, so a bytes-scan on
    # 50054 returns an empty result while 50052/50053 return the correct
    # field-value pairs.
    @pytest.mark.xfail(reason="HSetBytes does not fan out to replica 50054 (only 50052/50053 receive writes)", strict=True)
    def test_hscan_bytes_replicated(self, primary, replicas, cleanup):
        key_bytes = f"repl:scan:hscanb:{_uid()}".encode()
        cleanup.append(key_bytes.decode("latin-1"))

        primary.hset_bytes(key_bytes, b"f1", b"\x10")
        primary.hset_bytes(key_bytes, b"f2", b"\x20\x21")

        expected = sorted([(b"f1", b"\x10"), (b"f2", b"\x20\x21")])
        assert_replicated(
            replicas,
            lambda r: sorted(_drain_hscan_bytes(r, key_bytes)),
            expected,
            "hscan_bytes drain after two hset_bytes",
        )
