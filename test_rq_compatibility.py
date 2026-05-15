"""
test_rq_compatibility.py

Compatibility matrix: rustycluster.rq vs Python RQ public API.

Each test prints one of:
    [PASS]     feature_name
    [FAIL]     feature_name: reason
    [NOT_IMPL] feature_name: what is missing

Run:
    python test_rq_compatibility.py
"""

from __future__ import annotations

import sys
import time
import traceback
from datetime import datetime
from typing import Callable

from rustycluster import close_all, get_client
from rustycluster.rq import (
    FailedJobRegistry,
    FinishedJobRegistry,
    Job,
    JobStatus,
    Queue,
    Retry,
    StartedJobRegistry,
    Worker,
)
from rustycluster.rq.worker import get_current_job

# ── Result tracking ──────────────────────────────────────────────────────────

_results: list[tuple[str, str, str]] = []  # (status, name, detail)


def run_test(name: str, fn: Callable[..., None], *args) -> None:
    try:
        fn(*args)
        _results.append(("PASS", name, ""))
        print(f"[PASS]     {name}")
    except Exception as e:
        detail = str(e)
        _results.append(("FAIL", name, detail))
        print(f"[FAIL]     {name}: {detail}")
        traceback.print_exc()


def not_impl(name: str, reason: str) -> None:
    _results.append(("NOT_IMPL", name, reason))
    print(f"[NOT_IMPL] {name}: {reason}")


# ── Module-level callables (must be importable by Worker via __main__) ───────

def _add(x, y):
    return x + y


def _add_kwargs(x, *, offset=0):
    return x + offset


def _raises():
    raise ValueError("intentional failure")


def _slow(seconds):
    time.sleep(seconds)
    return "done"


_current_job_captured = []


def _capture_current_job():
    _current_job_captured.clear()
    _current_job_captured.append(get_current_job())
    return "ok"


# ── Cleanup helper ───────────────────────────────────────────────────────────

def _cleanup(db, queue_name: str, job_ids: list[str] | None = None) -> None:
    db.delete(f"rc:queue:{queue_name}")
    db.delete(f"rc:started:{queue_name}")
    db.delete(f"rc:finished:{queue_name}")
    db.delete(f"rc:failed:{queue_name}")
    for jid in (job_ids or []):
        db.delete(f"rc:job:{jid}")


# ════════════════════════════════════════════════════════════════════════════
# Section 1: Queue API
# ════════════════════════════════════════════════════════════════════════════

def _queue_name_property(db) -> None:
    q = Queue("compat:qname", connection=db)
    assert q.name == "compat:qname", f"got {q.name!r}"


def _queue_key_property(db) -> None:
    q = Queue("compat:qkey", connection=db)
    assert q.key == "rc:queue:compat:qkey", f"got {q.key!r}"


def _queue_len_empty(db) -> None:
    qname = "compat:len_empty"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    assert len(q) == 0, f"expected 0, got {len(q)}"


def _queue_len_after_enqueue(db) -> None:
    qname = "compat:len_grow"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    j1 = q.enqueue(_add, 1, 2)
    assert len(q) == 1
    j2 = q.enqueue(_add, 3, 4)
    assert len(q) == 2
    _cleanup(db, qname, [j1.id, j2.id])


def _queue_job_ids_order(db) -> None:
    qname = "compat:job_ids"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    j1 = q.enqueue(_add, 1, 0)
    j2 = q.enqueue(_add, 2, 0)
    ids = q.job_ids
    assert ids == [j1.id, j2.id], f"got {ids}"
    _cleanup(db, qname, [j1.id, j2.id])


def _queue_enqueue_with_kwargs(db) -> None:
    qname = "compat:enqueue_kwargs"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add_kwargs, 10, offset=5)
    Worker([q], connection=db).work(burst=True)
    job.refresh()
    assert job.result == 15, f"expected 15, got {job.result}"
    _cleanup(db, qname, [job.id])


def _queue_enqueue_custom_job_id(db) -> None:
    qname = "compat:custom_id"
    custom = "compat-custom-001"
    _cleanup(db, qname, [custom])
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 1, job_id=custom)
    assert job.id == custom, f"got {job.id!r}"
    assert custom in q.job_ids
    _cleanup(db, qname, [custom])


def _queue_enqueue_at_front(db) -> None:
    qname = "compat:at_front"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    j1 = q.enqueue(_add, 1, 0)
    j2 = q.enqueue(_add, 2, 0, at_front=True)
    ids = q.job_ids
    assert ids[0] == j2.id, f"expected j2 first, got {ids[0]!r}"
    _cleanup(db, qname, [j1.id, j2.id])


def _queue_enqueue_job_timeout_triggers(db) -> None:
    qname = "compat:timeout"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_slow, 5, job_timeout=1)
    Worker([q], connection=db).work(burst=True)
    job.refresh()
    assert job.get_status() == JobStatus.FAILED, f"expected FAILED after timeout, got {job.get_status()}"
    assert "timeout" in job.exc_info.lower() or "Timeout" in job.exc_info, \
        f"exc_info doesn't mention timeout: {job.exc_info!r}"
    _cleanup(db, qname, [job.id])


def _queue_enqueue_result_ttl_stored(db) -> None:
    qname = "compat:result_ttl"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 1, result_ttl=999)
    data = db.hget_all(f"rc:job:{job.id}")
    assert data.get("result_ttl") == "999", f"result_ttl not stored: {data.get('result_ttl')!r}"
    _cleanup(db, qname, [job.id])


def _queue_enqueue_failure_ttl_stored(db) -> None:
    qname = "compat:failure_ttl"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 1, failure_ttl=3600)
    data = db.hget_all(f"rc:job:{job.id}")
    assert data.get("failure_ttl") == "3600", f"failure_ttl not stored: {data.get('failure_ttl')!r}"
    _cleanup(db, qname, [job.id])


# ════════════════════════════════════════════════════════════════════════════
# Section 2: Job API
# ════════════════════════════════════════════════════════════════════════════

def _job_id_property(db) -> None:
    qname = "compat:job_id"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 2)
    assert isinstance(job.id, str) and job.id, "id should be a non-empty string"
    _cleanup(db, qname, [job.id])


def _job_origin_property(db) -> None:
    qname = "compat:job_origin"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 2)
    assert job.origin == qname, f"expected {qname!r}, got {job.origin!r}"
    _cleanup(db, qname, [job.id])


def _job_enqueued_at_is_datetime(db) -> None:
    qname = "compat:enqueued_at"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 2)
    assert isinstance(job.enqueued_at, datetime), \
        f"expected datetime, got {type(job.enqueued_at)}"
    _cleanup(db, qname, [job.id])


def _job_started_at_after_work(db) -> None:
    qname = "compat:started_at"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 2)
    Worker([q], connection=db).work(burst=True)
    job.refresh()
    assert isinstance(job.started_at, datetime), \
        f"expected datetime, got {type(job.started_at)}"
    _cleanup(db, qname, [job.id])


def _job_ended_at_after_work(db) -> None:
    qname = "compat:ended_at"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 2)
    Worker([q], connection=db).work(burst=True)
    job.refresh()
    assert isinstance(job.ended_at, datetime), \
        f"expected datetime, got {type(job.ended_at)}"
    _cleanup(db, qname, [job.id])


def _job_result_after_work(db) -> None:
    qname = "compat:result"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 10, 32)
    Worker([q], connection=db).work(burst=True)
    job.refresh()
    assert job.result == 42, f"expected 42, got {job.result}"
    _cleanup(db, qname, [job.id])


def _job_exc_info_on_failure(db) -> None:
    qname = "compat:exc_info"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_raises)
    Worker([q], connection=db).work(burst=True)
    job.refresh()
    assert "ValueError" in job.exc_info, \
        f"exc_info missing ValueError: {job.exc_info!r}"
    _cleanup(db, qname, [job.id])


def _job_get_status_queued(db) -> None:
    qname = "compat:status_queued"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 2)
    assert job.get_status() == JobStatus.QUEUED
    _cleanup(db, qname, [job.id])


def _job_get_status_finished(db) -> None:
    qname = "compat:status_finished"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 2)
    Worker([q], connection=db).work(burst=True)
    assert job.get_status() == JobStatus.FINISHED
    _cleanup(db, qname, [job.id])


def _job_get_status_failed(db) -> None:
    qname = "compat:status_failed"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_raises)
    Worker([q], connection=db).work(burst=True)
    assert job.get_status() == JobStatus.FAILED
    _cleanup(db, qname, [job.id])


def _job_cancel_sets_canceled(db) -> None:
    qname = "compat:cancel_status"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 2)
    job.cancel()
    assert job.get_status() == JobStatus.CANCELED, \
        f"expected CANCELED, got {job.get_status()}"
    _cleanup(db, qname, [job.id])


def _job_cancel_removes_from_queue(db) -> None:
    qname = "compat:cancel_remove"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 2)
    job.cancel()
    assert job.id not in q.job_ids, "canceled job should be removed from queue list"
    _cleanup(db, qname, [job.id])


def _job_refresh_updates_fields(db) -> None:
    qname = "compat:refresh"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 3, 4)
    assert job.result is None, "result should be None before work"
    Worker([q], connection=db).work(burst=True)
    job.refresh()
    assert job.result == 7, f"expected 7 after refresh, got {job.result}"
    _cleanup(db, qname, [job.id])


def _job_fetch_by_id(db) -> None:
    qname = "compat:fetch"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    original = q.enqueue(_add, 5, 6)
    fetched = Job.fetch(original.id, connection=db)
    assert fetched.id == original.id
    assert fetched.get_status() == JobStatus.QUEUED
    _cleanup(db, qname, [original.id])


# ════════════════════════════════════════════════════════════════════════════
# Section 3: Worker API
# ════════════════════════════════════════════════════════════════════════════

def _worker_burst_mode_exits(db) -> None:
    qname = "compat:burst"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    q.enqueue(_add, 1, 2)
    processed = Worker([q], connection=db).work(burst=True)
    assert processed is True, "work() should return True when jobs were processed"


def _worker_executes_and_stores_result(db) -> None:
    qname = "compat:worker_result"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 20, 22)
    Worker([q], connection=db).work(burst=True)
    job.refresh()
    assert job.result == 42, f"expected 42, got {job.result}"
    _cleanup(db, qname, [job.id])


def _worker_handles_failure(db) -> None:
    qname = "compat:worker_fail"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_raises)
    Worker([q], connection=db).work(burst=True)
    assert job.get_status() == JobStatus.FAILED
    _cleanup(db, qname, [job.id])


def _worker_timeout_enforcement(db) -> None:
    qname = "compat:worker_timeout"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_slow, 10, job_timeout=1)
    Worker([q], connection=db).work(burst=True)
    assert job.get_status() == JobStatus.FAILED, \
        f"expected FAILED on timeout, got {job.get_status()}"
    _cleanup(db, qname, [job.id])


def _worker_get_current_job_module_fn(db) -> None:
    qname = "compat:current_job"
    _cleanup(db, qname)
    _current_job_captured.clear()
    q = Queue(qname, connection=db)
    job = q.enqueue(_capture_current_job)
    Worker([q], connection=db).work(burst=True)
    assert len(_current_job_captured) == 1, "get_current_job() was never called"
    captured = _current_job_captured[0]
    assert captured is not None, "get_current_job() returned None during execution"
    assert captured.id == job.id, f"expected {job.id!r}, got {captured.id!r}"
    _cleanup(db, qname, [job.id])


def _worker_multi_queue_polling(db) -> None:
    q1name = "compat:mq1"
    q2name = "compat:mq2"
    _cleanup(db, q1name)
    _cleanup(db, q2name)
    q1 = Queue(q1name, connection=db)
    q2 = Queue(q2name, connection=db)
    j1 = q1.enqueue(_add, 1, 0)
    j2 = q2.enqueue(_add, 2, 0)
    Worker([q1, q2], connection=db).work(burst=True)
    assert j1.get_status() == JobStatus.FINISHED, f"j1 status: {j1.get_status()}"
    assert j2.get_status() == JobStatus.FINISHED, f"j2 status: {j2.get_status()}"
    _cleanup(db, q1name, [j1.id])
    _cleanup(db, q2name, [j2.id])


# ════════════════════════════════════════════════════════════════════════════
# Section 4: Retry
# ════════════════════════════════════════════════════════════════════════════

def _retry_max_and_interval_attrs(db) -> None:
    r = Retry(max=3, interval=[0, 5, 30])
    assert r.max == 3, f"max: {r.max}"
    assert r.interval == [0, 5, 30], f"interval: {r.interval}"


def _retry_get_interval(db) -> None:
    r = Retry(max=3, interval=[0, 5, 30])
    assert r.get_interval(0) == 0
    assert r.get_interval(1) == 5
    assert r.get_interval(2) == 30
    assert r.get_interval(99) == 30  # clamps to last element


def _retry_requeues_on_failure(db) -> None:
    qname = "compat:retry_requeue"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_raises, retry=Retry(max=1))
    Worker([q], connection=db).work(burst=True)
    status = job.get_status()
    assert status == JobStatus.FAILED, f"expected FAILED after retries, got {status}"
    _cleanup(db, qname, [job.id])


def _retry_exhaustion_moves_to_failed(db) -> None:
    qname = "compat:retry_exhaust"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_raises, retry=Retry(max=2))
    Worker([q], connection=db).work(burst=True)
    assert job.get_status() == JobStatus.FAILED
    reg = FailedJobRegistry(name=qname, connection=db)
    assert reg.contains(job.id), "job not in FailedJobRegistry after retry exhaustion"
    _cleanup(db, qname, [job.id])


def _retry_scalar_interval(db) -> None:
    r = Retry(max=2, interval=10)
    assert r.interval == [10], f"scalar interval should become [10], got {r.interval}"
    assert r.get_interval(0) == 10
    assert r.get_interval(5) == 10  # always same value


# ════════════════════════════════════════════════════════════════════════════
# Section 5: Registries
# ════════════════════════════════════════════════════════════════════════════

def _finished_registry_after_success(db) -> None:
    qname = "compat:reg_finished"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 1)
    Worker([q], connection=db).work(burst=True)
    reg = FinishedJobRegistry(name=qname, connection=db)
    assert len(reg) == 1, f"expected 1 finished, got {len(reg)}"
    assert reg.contains(job.id)
    _cleanup(db, qname, [job.id])


def _failed_registry_after_failure(db) -> None:
    qname = "compat:reg_failed"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_raises)
    Worker([q], connection=db).work(burst=True)
    reg = FailedJobRegistry(name=qname, connection=db)
    assert len(reg) == 1, f"expected 1 failed, got {len(reg)}"
    assert reg.contains(job.id)
    _cleanup(db, qname, [job.id])


def _registry_get_job_ids(db) -> None:
    qname = "compat:reg_ids"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    j1 = q.enqueue(_add, 1, 0)
    j2 = q.enqueue(_add, 2, 0)
    Worker([q], connection=db).work(burst=True)
    reg = FinishedJobRegistry(name=qname, connection=db)
    ids = reg.get_job_ids()
    assert j1.id in ids, f"{j1.id} not in {ids}"
    assert j2.id in ids, f"{j2.id} not in {ids}"
    _cleanup(db, qname, [j1.id, j2.id])


def _registry_len(db) -> None:
    qname = "compat:reg_len"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    j1 = q.enqueue(_add, 1, 0)
    j2 = q.enqueue(_add, 2, 0)
    Worker([q], connection=db).work(burst=True)
    reg = FinishedJobRegistry(name=qname, connection=db)
    assert len(reg) == 2, f"expected 2, got {len(reg)}"
    _cleanup(db, qname, [j1.id, j2.id])


def _registry_contains(db) -> None:
    qname = "compat:reg_contains"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 3, 4)
    Worker([q], connection=db).work(burst=True)
    reg = FinishedJobRegistry(name=qname, connection=db)
    assert reg.contains(job.id), "contains() returned False for finished job"
    assert not reg.contains("nonexistent-job-id"), "contains() should return False for unknown id"
    _cleanup(db, qname, [job.id])


# ════════════════════════════════════════════════════════════════════════════
# Section 6: Silently ignored features
# ════════════════════════════════════════════════════════════════════════════

def _depends_on_accepted_not_enforced(db) -> None:
    qname = "compat:depends_on"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    j1 = q.enqueue(_add, 1, 0)
    # depends_on accepted without error, but NOT enforced — j2 runs immediately
    j2 = q.enqueue(_add, 2, 0, depends_on=j1)
    Worker([q], connection=db).work(burst=True)
    # Both finish — dependency was silently ignored
    assert j1.get_status() == JobStatus.FINISHED
    assert j2.get_status() == JobStatus.FINISHED
    _cleanup(db, qname, [j1.id, j2.id])


def _on_success_accepted_not_called(db) -> None:
    called = []

    def success_cb(job, conn, result, *args, **kwargs):
        called.append(result)

    qname = "compat:on_success"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 1, on_success=success_cb)
    Worker([q], connection=db).work(burst=True)
    assert job.get_status() == JobStatus.FINISHED
    # on_success is accepted but never called — called list stays empty
    assert len(called) == 0, \
        f"on_success was unexpectedly called: {called}"
    _cleanup(db, qname, [job.id])


def _on_failure_accepted_not_called(db) -> None:
    called = []

    def failure_cb(job, conn, typ, val, tb):
        called.append(str(val))

    qname = "compat:on_failure"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_raises, on_failure=failure_cb)
    Worker([q], connection=db).work(burst=True)
    assert job.get_status() == JobStatus.FAILED
    # on_failure is accepted but never called
    assert len(called) == 0, \
        f"on_failure was unexpectedly called: {called}"
    _cleanup(db, qname, [job.id])


def _result_ttl_enforced(db) -> None:
    qname = "compat:result_ttl_enforce"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 1, result_ttl=1)
    Worker([q], connection=db).work(burst=True)
    time.sleep(2)
    data = db.hget_all(f"rc:job:{job.id}")
    assert not data, "job hash should have expired after result_ttl=1s"


# ════════════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════════════

def main() -> int:
    db = get_client("DB0")
    try:
        print("\n" + "═" * 68)
        print("  rustycluster.rq — Python RQ Compatibility Matrix")
        print("═" * 68 + "\n")

        # ── Section 1: Queue API ─────────────────────────────────────────
        print("── Section 1: Queue API ──────────────────────────────────────────")
        run_test("queue.name", _queue_name_property, db)
        run_test("queue.key", _queue_key_property, db)
        run_test("queue.__len__() empty", _queue_len_empty, db)
        run_test("queue.__len__() grows on enqueue", _queue_len_after_enqueue, db)
        run_test("queue.job_ids order (FIFO)", _queue_job_ids_order, db)
        run_test("queue.enqueue() with kwargs", _queue_enqueue_with_kwargs, db)
        run_test("queue.enqueue() custom job_id", _queue_enqueue_custom_job_id, db)
        run_test("queue.enqueue() at_front=True", _queue_enqueue_at_front, db)
        run_test("queue.enqueue() job_timeout enforced", _queue_enqueue_job_timeout_triggers, db)
        run_test("queue.enqueue() result_ttl stored", _queue_enqueue_result_ttl_stored, db)
        run_test("queue.enqueue() failure_ttl stored", _queue_enqueue_failure_ttl_stored, db)
        not_impl("queue.empty()", "method not implemented — no way to flush all jobs from queue")
        not_impl("queue.count (property)", "only __len__() exists; rq also exposes .count as an alias")
        not_impl("queue.enqueue() description public", "description stored as _description (private); no Job.description property")
        print()

        # ── Section 2: Job API ───────────────────────────────────────────
        print("── Section 2: Job API ────────────────────────────────────────────")
        run_test("job.id", _job_id_property, db)
        run_test("job.origin", _job_origin_property, db)
        run_test("job.enqueued_at is datetime", _job_enqueued_at_is_datetime, db)
        run_test("job.started_at set after work", _job_started_at_after_work, db)
        run_test("job.ended_at set after work", _job_ended_at_after_work, db)
        run_test("job.result after success", _job_result_after_work, db)
        run_test("job.exc_info on failure", _job_exc_info_on_failure, db)
        run_test("job.get_status() → QUEUED", _job_get_status_queued, db)
        run_test("job.get_status() → FINISHED", _job_get_status_finished, db)
        run_test("job.get_status() → FAILED", _job_get_status_failed, db)
        run_test("job.cancel() sets CANCELED", _job_cancel_sets_canceled, db)
        run_test("job.cancel() removes from queue list", _job_cancel_removes_from_queue, db)
        run_test("job.refresh() reloads fields", _job_refresh_updates_fields, db)
        run_test("Job.fetch(id, connection)", _job_fetch_by_id, db)
        not_impl("job.description (public property)", "_description is private; rq exposes it publicly")
        not_impl("job.meta (dict)", "arbitrary metadata dict not implemented")
        not_impl("job.delete() (public method)", "only _delete() exists as a private helper")
        not_impl("job.timeout (public property)", "_job_timeout is private; rq exposes .timeout")
        not_impl("job.save_meta()", "no meta support — method does not exist")
        print()

        # ── Section 3: Worker API ────────────────────────────────────────
        print("── Section 3: Worker API ─────────────────────────────────────────")
        run_test("worker.work(burst=True) exits", _worker_burst_mode_exits, db)
        run_test("worker executes job and stores result", _worker_executes_and_stores_result, db)
        run_test("worker handles job failure", _worker_handles_failure, db)
        run_test("worker enforces job_timeout", _worker_timeout_enforcement, db)
        run_test("get_current_job() (module-level) during execution", _worker_get_current_job_module_fn, db)
        run_test("worker polls multiple queues (round-robin)", _worker_multi_queue_polling, db)
        not_impl("Worker.get_current_job() (instance method)", "only module-level get_current_job() exists")
        not_impl("Worker.get_queue_names()", "method not implemented")
        not_impl("worker.work(with_scheduler=True) — scheduler", "with_scheduler accepted but ignored; no scheduler")
        print()

        # ── Section 4: Retry ─────────────────────────────────────────────
        print("── Section 4: Retry ──────────────────────────────────────────────")
        run_test("Retry.max and .interval attributes", _retry_max_and_interval_attrs, db)
        run_test("Retry.get_interval() — list intervals", _retry_get_interval, db)
        run_test("Retry.get_interval() — scalar interval", _retry_scalar_interval, db)
        run_test("retry re-enqueues on failure", _retry_requeues_on_failure, db)
        run_test("retry exhaustion moves job to FailedJobRegistry", _retry_exhaustion_moves_to_failed, db)
        print()

        # ── Section 5: Registries ────────────────────────────────────────
        print("── Section 5: Registries ─────────────────────────────────────────")
        run_test("FinishedJobRegistry after success", _finished_registry_after_success, db)
        run_test("FailedJobRegistry after failure", _failed_registry_after_failure, db)
        run_test("registry.get_job_ids()", _registry_get_job_ids, db)
        run_test("registry.__len__()", _registry_len, db)
        run_test("registry.contains()", _registry_contains, db)
        not_impl("CanceledJobRegistry", "canceled jobs are not tracked in a registry")
        not_impl("DeferredJobRegistry", "no job dependency support")
        not_impl("ScheduledJobRegistry", "no job scheduler")
        print()

        # ── Section 6: Silently ignored features ─────────────────────────
        print("── Section 6: Silently Ignored Features ──────────────────────────")
        run_test("depends_on accepted (silently ignored)", _depends_on_accepted_not_enforced, db)
        run_test("on_success accepted (silently not called)", _on_success_accepted_not_called, db)
        run_test("on_failure accepted (silently not called)", _on_failure_accepted_not_called, db)
        run_test("result_ttl enforced — key expires after ttl", _result_ttl_enforced, db)
        print()

    finally:
        close_all()

    # ── Compatibility Matrix Summary ──────────────────────────────────────
    passed = [r for r in _results if r[0] == "PASS"]
    failed = [r for r in _results if r[0] == "FAIL"]
    not_implv = [r for r in _results if r[0] == "NOT_IMPL"]
    total = len(_results)

    bar_width = 30

    def bar(count: int) -> str:
        filled = round(bar_width * count / total) if total else 0
        return "█" * filled + "░" * (bar_width - filled)

    pct = lambda n: f"{round(100 * n / total):3d}%" if total else "  0%"

    print("╔" + "═" * 66 + "╗")
    print("║  RQ Compatibility Matrix" + " " * 41 + "║")
    print("╠" + "═" * 66 + "╣")
    print(f"║  PASS      {len(passed):3d}   {bar(len(passed))}  {pct(len(passed))}  ║")
    print(f"║  FAIL      {len(failed):3d}   {bar(len(failed))}  {pct(len(failed))}  ║")
    print(f"║  NOT_IMPL  {len(not_implv):3d}   {bar(len(not_implv))}  {pct(len(not_implv))}  ║")
    print(f"║  TOTAL     {total:3d}" + " " * 39 + "║")
    print("╚" + "═" * 66 + "╝")

    if failed:
        print("\nFailed tests:")
        for _, name, detail in failed:
            print(f"  [FAIL] {name}: {detail}")

    if not_implv:
        print("\nNot implemented (gaps vs Python RQ):")
        for _, name, detail in not_implv:
            print(f"  [NOT_IMPL] {name}: {detail}")

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
