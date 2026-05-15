"""
SampleTestQueue.py

Runnable demo of the rustycluster client's queue/list/stream operations
against a locally running cluster, configured via rustycluster.yaml -> DB0.

Methods exercised:
    LPUSH, RPUSH, LPOP, LRANGE, LTRIM, XADD, XREAD

Run:
    python SampleTestQueue.py
"""

from __future__ import annotations

import sys
import traceback
from typing import Callable

from rustycluster import close_all, get_client


_passed = 0
_failed = 0


def run(name: str, fn: Callable[..., None], *args) -> None:
    global _passed, _failed
    try:
        fn(*args)
    except Exception as e:
        _failed += 1
        print(f"[FAIL] {name}: {e}")
        traceback.print_exc()
        return
    _passed += 1
    print(f"[PASS] {name}")


# ─── List scenarios ───────────────────────────────────────────────────────────

def scenario_lpush_prepends(db) -> None:
    key = "qtest:lpush"
    db.delete(key)
    try:
        length = db.lpush(key, "a", "b", "c")
        assert length == 3, f"expected length 3, got {length}"
        contents = db.lrange(key, 0, -1)
        assert contents == ["c", "b", "a"], f"expected ['c','b','a'], got {contents}"
    finally:
        db.delete(key)


def scenario_rpush_appends(db) -> None:
    key = "qtest:rpush"
    db.delete(key)
    try:
        length = db.rpush(key, "x", "y", "z")
        assert length == 3, f"expected length 3, got {length}"
        contents = db.lrange(key, 0, -1)
        assert contents == ["x", "y", "z"], f"expected ['x','y','z'], got {contents}"
    finally:
        db.delete(key)


def scenario_lpop_default_single(db) -> None:
    key = "qtest:lpop1"
    db.delete(key)
    try:
        db.rpush(key, "1", "2", "3")
        popped = db.lpop(key)
        assert popped == ["1"], f"expected ['1'], got {popped}"
        remaining = db.lrange(key, 0, -1)
        assert remaining == ["2", "3"], f"expected ['2','3'], got {remaining}"
    finally:
        db.delete(key)


def scenario_lpop_with_count(db) -> None:
    key = "qtest:lpop2"
    db.delete(key)
    try:
        db.rpush(key, "a", "b", "c", "d")
        popped = db.lpop(key, count=2)
        assert popped == ["a", "b"], f"expected ['a','b'], got {popped}"
        remaining = db.lrange(key, 0, -1)
        assert remaining == ["c", "d"], f"expected ['c','d'], got {remaining}"
    finally:
        db.delete(key)


def scenario_lpop_missing_returns_empty(db) -> None:
    key = "qtest:lpop_missing"
    db.delete(key)
    popped = db.lpop(key)
    assert popped == [], f"expected [], got {popped}"


def scenario_lrange_full_and_slice(db) -> None:
    key = "qtest:lrange"
    db.delete(key)
    try:
        db.rpush(key, "a", "b", "c", "d", "e")
        full = db.lrange(key, 0, -1)
        assert full == ["a", "b", "c", "d", "e"], f"full range mismatch: {full}"
        # stop is inclusive (Redis semantics)
        sliced = db.lrange(key, 1, 3)
        assert sliced == ["b", "c", "d"], f"slice mismatch: {sliced}"
    finally:
        db.delete(key)


def scenario_ltrim_keeps_range(db) -> None:
    key = "qtest:ltrim"
    db.delete(key)
    try:
        db.rpush(key, "a", "b", "c", "d", "e")
        ok = db.ltrim(key, 1, 3)
        assert ok is True, f"ltrim returned {ok}"
        contents = db.lrange(key, 0, -1)
        assert contents == ["b", "c", "d"], f"expected ['b','c','d'], got {contents}"
    finally:
        db.delete(key)


# ─── Stream scenarios ─────────────────────────────────────────────────────────

def scenario_xadd_auto_id_and_xread(db) -> None:
    key = "qtest:stream1"
    db.delete(key)
    try:
        eid = db.xadd(key, {"f1": "v1", "f2": "v2"})
        assert eid and eid != "*", f"expected resolved id, got {eid!r}"
        entries = db.xread([(key, "0")])
        assert len(entries) == 1, f"expected 1 entry, got {len(entries)}: {entries}"
        ekey, returned_id, fields = entries[0]
        assert ekey == key, f"stream key mismatch: {ekey}"
        assert returned_id == eid, f"id mismatch: {returned_id} != {eid}"
        assert fields == {"f1": "v1", "f2": "v2"}, f"fields mismatch: {fields}"
    finally:
        db.delete(key)


def scenario_xread_after_last_id_is_empty(db) -> None:
    key = "qtest:stream2"
    db.delete(key)
    try:
        eid = db.xadd(key, {"k": "v"})
        entries = db.xread([(key, eid)])
        assert entries == [], f"expected empty, got {entries}"
    finally:
        db.delete(key)


def scenario_xread_with_count_limits(db) -> None:
    key = "qtest:stream3"
    db.delete(key)
    try:
        db.xadd(key, {"a": "1"})
        db.xadd(key, {"a": "2"})
        db.xadd(key, {"a": "3"})
        entries = db.xread([(key, "0")], count=2)
        assert len(entries) == 2, f"expected 2 entries, got {len(entries)}: {entries}"
    finally:
        db.delete(key)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> int:
    db0 = get_client("DB0")
    try:
        # Lists
        run("lpush_prepends", scenario_lpush_prepends, db0)
        run("rpush_appends", scenario_rpush_appends, db0)
        run("lpop_default_single", scenario_lpop_default_single, db0)
        run("lpop_with_count", scenario_lpop_with_count, db0)
        run("lpop_missing_returns_empty", scenario_lpop_missing_returns_empty, db0)
        run("lrange_full_and_slice", scenario_lrange_full_and_slice, db0)
        run("ltrim_keeps_range", scenario_ltrim_keeps_range, db0)
        # Streams
        run("xadd_auto_id_and_xread", scenario_xadd_auto_id_and_xread, db0)
        run("xread_after_last_id_is_empty", scenario_xread_after_last_id_is_empty, db0)
        run("xread_with_count_limits", scenario_xread_with_count_limits, db0)
    finally:
        close_all()

    total = _passed + _failed
    print(f"\nSummary: {_passed}/{total} scenarios passed")
    return 0 if _failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
