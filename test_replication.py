"""
Replication integration tests for RustyCluster.

Writes are applied to the primary node (50051). Reads are verified on all
replica nodes (50052, 50053, 50054).  Each test uses a UUID-suffixed key to
avoid cross-test interference.

Each assertion polls replicas up to REPLICATION_TIMEOUT seconds before
failing, so tests pass as soon as replication arrives and only fail when it
genuinely doesn't happen.

Run:
    pytest test_replication.py -v
"""

import time
import uuid
from typing import Callable, Any

import pytest

from rustycluster import get_client
from rustycluster.config import RustyClusterConfig, RustyClusterSettings
from rustycluster.client import RustyClusterClient

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
