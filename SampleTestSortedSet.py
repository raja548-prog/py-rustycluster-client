"""
SampleTestSortedSet.py

Runnable demo of the rustycluster client's sorted-set and blocking-pop
operations against a locally running cluster, configured via
rustycluster.yaml -> DB0.

Methods exercised:
    ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZSCORE, ZCARD,
    BLPOP, BRPOP

Run:
    python SampleTestSortedSet.py
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


# ─── Sorted-set scenarios ─────────────────────────────────────────────────────

def scenario_zadd_new_member(db) -> None:
    key = "ztest:zadd"
    db.zrem(key, "a")
    added = db.zadd(key, 1.0, "a")
    assert added == 1, f"expected 1 (new member), got {added}"
    db.zrem(key, "a")


def scenario_zadd_score_update(db) -> None:
    key = "ztest:update"
    db.zrem(key, "a")
    db.zadd(key, 1.0, "a")
    added = db.zadd(key, 99.0, "a")
    assert added == 0, f"expected 0 (score update), got {added}"
    score = db.zscore(key, "a")
    assert score == pytest_approx(99.0), f"expected score 99.0, got {score}"
    db.zrem(key, "a")


def scenario_zcard(db) -> None:
    key = "ztest:zcard"
    db.zrem(key, "a", "b", "c")
    db.zadd(key, 1.0, "a")
    db.zadd(key, 2.0, "b")
    db.zadd(key, 3.0, "c")
    card = db.zcard(key)
    assert card == 3, f"expected cardinality 3, got {card}"
    db.zrem(key, "a", "b", "c")


def scenario_zscore_found_and_missing(db) -> None:
    key = "ztest:zscore"
    db.zrem(key, "pi")
    db.zadd(key, 3.14, "pi")
    score = db.zscore(key, "pi")
    assert abs(score - 3.14) < 1e-9, f"expected ~3.14, got {score}"
    missing = db.zscore(key, "ghost")
    assert missing is None, f"expected None for missing member, got {missing}"
    db.zrem(key, "pi")


def scenario_zrange_ordered_by_score(db) -> None:
    key = "ztest:zrange"
    db.zrem(key, "a", "b", "c")
    db.zadd(key, 3.0, "c")
    db.zadd(key, 1.0, "a")
    db.zadd(key, 2.0, "b")
    members = db.zrange(key, 0, -1)
    assert members == ["a", "b", "c"], f"expected ['a','b','c'], got {members}"
    db.zrem(key, "a", "b", "c")


def scenario_zrange_with_scores(db) -> None:
    key = "ztest:zrange_ws"
    db.zrem(key, "x", "y")
    db.zadd(key, 10.0, "x")
    db.zadd(key, 20.0, "y")
    result = db.zrange(key, 0, -1, with_scores=True)
    assert result == [("x", 10.0), ("y", 20.0)], f"unexpected result: {result}"
    db.zrem(key, "x", "y")


def scenario_zrange_by_score(db) -> None:
    key = "ztest:zrbs"
    db.zrem(key, "a", "b", "c")
    db.zadd(key, 1.0, "a")
    db.zadd(key, 5.0, "b")
    db.zadd(key, 10.0, "c")
    members = db.zrange_by_score(key, 1.0, 6.0)
    assert members == ["a", "b"], f"expected ['a','b'], got {members}"
    db.zrem(key, "a", "b", "c")


def scenario_zrange_by_score_with_scores(db) -> None:
    key = "ztest:zrbs_ws"
    db.zrem(key, "a", "b")
    db.zadd(key, 2.0, "a")
    db.zadd(key, 4.0, "b")
    result = db.zrange_by_score(key, 0, 10, with_scores=True)
    assert result == [("a", 2.0), ("b", 4.0)], f"unexpected result: {result}"
    db.zrem(key, "a", "b")


def scenario_zrange_by_score_with_limit(db) -> None:
    key = "ztest:zrbs_lim"
    db.zrem(key, "a", "b", "c", "d")
    db.zadd(key, 1.0, "a")
    db.zadd(key, 2.0, "b")
    db.zadd(key, 3.0, "c")
    db.zadd(key, 4.0, "d")
    # skip first 1, return next 2
    members = db.zrange_by_score(key, 0, 10, offset=1, count=2)
    assert members == ["b", "c"], f"expected ['b','c'], got {members}"
    db.zrem(key, "a", "b", "c", "d")


def scenario_zrem_multiple(db) -> None:
    key = "ztest:zrem"
    db.zrem(key, "a", "b", "c")
    db.zadd(key, 1.0, "a")
    db.zadd(key, 2.0, "b")
    db.zadd(key, 3.0, "c")
    removed = db.zrem(key, "a", "c", "ghost")
    assert removed == 2, f"expected 2 removed (ghost ignored), got {removed}"
    remaining = db.zrange(key, 0, -1)
    assert remaining == ["b"], f"expected ['b'], got {remaining}"
    db.zrem(key, "b")


# ─── Blocking-pop scenarios ───────────────────────────────────────────────────

def scenario_blpop_returns_from_non_empty_list(db) -> None:
    key = "ztest:blq"
    db.delete(key)
    db.rpush(key, "first", "second")
    result = db.blpop(key, timeout=1.0)
    assert result == (key, "first"), f"expected ({key!r}, 'first'), got {result}"
    db.delete(key)


def scenario_blpop_checks_keys_in_order(db) -> None:
    key_empty = "ztest:blq_empty"
    key_data = "ztest:blq_data"
    db.delete(key_empty)
    db.delete(key_data)
    db.rpush(key_data, "value")
    result = db.blpop(key_empty, key_data, timeout=1.0)
    assert result == (key_data, "value"), f"unexpected result: {result}"
    db.delete(key_data)


def scenario_blpop_empty_returns_none(db) -> None:
    key = "ztest:blq_none"
    db.delete(key)
    result = db.blpop(key, timeout=0.1)
    assert result is None, f"expected None on timeout, got {result}"


def scenario_brpop_returns_from_tail(db) -> None:
    key = "ztest:brq"
    db.delete(key)
    db.rpush(key, "first", "last")
    result = db.brpop(key, timeout=1.0)
    assert result == (key, "last"), f"expected ({key!r}, 'last'), got {result}"
    db.delete(key)


def scenario_brpop_empty_returns_none(db) -> None:
    key = "ztest:brq_none"
    db.delete(key)
    result = db.brpop(key, timeout=0.1)
    assert result is None, f"expected None on timeout, got {result}"


# ─── Minimal approx helper (avoids pytest dependency at runtime) ──────────────

def pytest_approx(expected, rel=1e-6):
    class _Approx:
        def __eq__(self, actual):
            return abs(actual - expected) <= rel * max(abs(expected), 1e-12)
        def __repr__(self):
            return f"~{expected}"
    return _Approx()


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> int:
    db0 = get_client("DB0")
    try:
        # Sorted set
        run("zadd_new_member", scenario_zadd_new_member, db0)
        run("zadd_score_update", scenario_zadd_score_update, db0)
        run("zcard", scenario_zcard, db0)
        run("zscore_found_and_missing", scenario_zscore_found_and_missing, db0)
        run("zrange_ordered_by_score", scenario_zrange_ordered_by_score, db0)
        run("zrange_with_scores", scenario_zrange_with_scores, db0)
        run("zrange_by_score", scenario_zrange_by_score, db0)
        run("zrange_by_score_with_scores", scenario_zrange_by_score_with_scores, db0)
        run("zrange_by_score_with_limit", scenario_zrange_by_score_with_limit, db0)
        run("zrem_multiple", scenario_zrem_multiple, db0)
        # Blocking pops
        run("blpop_returns_from_non_empty_list", scenario_blpop_returns_from_non_empty_list, db0)
        run("blpop_checks_keys_in_order", scenario_blpop_checks_keys_in_order, db0)
        run("blpop_empty_returns_none", scenario_blpop_empty_returns_none, db0)
        run("brpop_returns_from_tail", scenario_brpop_returns_from_tail, db0)
        run("brpop_empty_returns_none", scenario_brpop_empty_returns_none, db0)
    finally:
        close_all()

    total = _passed + _failed
    print(f"\nSummary: {_passed}/{total} scenarios passed")
    return 0 if _failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
