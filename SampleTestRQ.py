"""
SampleTestRQ.py

Runnable demo of the rustycluster.rq job-queue layer against a locally
running cluster, configured via rustycluster.yaml -> DB0.

Classes / functions exercised:
    Queue, Worker, Job, JobStatus, Retry,
    StartedJobRegistry, FinishedJobRegistry, FailedJobRegistry

Run:
    python SampleTestRQ.py
"""

from __future__ import annotations

import sys
import traceback
from typing import Callable

from rustycluster import close_all, get_client
from rustycluster.rq import (
    FailedJobRegistry,
    FinishedJobRegistry,
    Job,
    JobStatus,
    Queue,
    Retry,
    Worker,
)


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


# ─── Module-level callables for Worker ───────────────────────────────────────
# Must be at module scope so _ref_to_func can resolve them via
# importlib.import_module("__main__") when the script is run directly.

def _add(x, y):
    return x + y


def _raises():
    raise ValueError("intentional failure")


# ─── Cleanup helper ───────────────────────────────────────────────────────────

def _cleanup(db, queue_name: str, job_ids: list[str] | None = None) -> None:
    db.delete(f"rc:queue:{queue_name}")
    db.delete(f"rc:started:{queue_name}")
    db.delete(f"rc:finished:{queue_name}")
    db.delete(f"rc:failed:{queue_name}")
    for jid in (job_ids or []):
        db.delete(f"rc:job:{jid}")


# ─── Queue property scenarios ─────────────────────────────────────────────────

def scenario_queue_name_and_key(db) -> None:
    qname = "rqtest:props"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    assert q.name == qname, f"expected name {qname!r}, got {q.name!r}"
    assert q.key == f"rc:queue:{qname}", f"unexpected key: {q.key}"


def scenario_queue_length_empty(db) -> None:
    qname = "rqtest:empty"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    assert len(q) == 0, f"expected empty queue, got len={len(q)}"


def scenario_queue_len_grows_on_enqueue(db) -> None:
    qname = "rqtest:len_grow"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    j1 = q.enqueue(_add, 1, 2)
    assert len(q) == 1, f"expected len=1, got {len(q)}"
    j2 = q.enqueue(_add, 3, 4)
    assert len(q) == 2, f"expected len=2, got {len(q)}"
    _cleanup(db, qname, [j1.id, j2.id])


def scenario_queue_job_ids(db) -> None:
    qname = "rqtest:job_ids"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    j1 = q.enqueue(_add, 1, 2)
    j2 = q.enqueue(_add, 3, 4)
    ids = q.job_ids
    assert ids == [j1.id, j2.id], f"expected [{j1.id!r}, {j2.id!r}], got {ids}"
    _cleanup(db, qname, [j1.id, j2.id])


# ─── Job creation & metadata scenarios ───────────────────────────────────────

def scenario_enqueue_returns_queued_job(db) -> None:
    qname = "rqtest:enqueue_status"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 5, 6)
    assert isinstance(job, Job), f"expected Job, got {type(job)}"
    status = job.get_status()
    assert status == JobStatus.QUEUED, f"expected QUEUED, got {status}"
    _cleanup(db, qname, [job.id])


def scenario_job_fetch_by_id(db) -> None:
    qname = "rqtest:fetch"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    original = q.enqueue(_add, 7, 8)
    fetched = Job.fetch(original.id, connection=db)
    assert fetched.id == original.id, f"id mismatch: {fetched.id} != {original.id}"
    assert fetched.get_status() == JobStatus.QUEUED, "fetched job should be QUEUED"
    _cleanup(db, qname, [original.id])


def scenario_job_origin_and_timestamp(db) -> None:
    qname = "rqtest:origin"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 1)
    assert job.origin == qname, f"expected origin {qname!r}, got {job.origin!r}"
    assert job.enqueued_at is not None, "enqueued_at should be set"
    _cleanup(db, qname, [job.id])


def scenario_custom_job_id(db) -> None:
    qname = "rqtest:custom_id"
    custom_id = "my-custom-job-id-001"
    _cleanup(db, qname, [custom_id])
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 2, 2, job_id=custom_id)
    assert job.id == custom_id, f"expected id {custom_id!r}, got {job.id!r}"
    assert custom_id in q.job_ids, "custom id not found in queue"
    _cleanup(db, qname, [custom_id])


def scenario_at_front_enqueue(db) -> None:
    qname = "rqtest:at_front"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    j1 = q.enqueue(_add, 1, 0)
    j2 = q.enqueue(_add, 2, 0, at_front=True)
    ids = q.job_ids
    assert ids[0] == j2.id, f"expected j2 at front, got {ids[0]!r}"
    assert ids[1] == j1.id, f"expected j1 second, got {ids[1]!r}"
    _cleanup(db, qname, [j1.id, j2.id])


# ─── Worker execution scenarios ───────────────────────────────────────────────

def scenario_worker_executes_job(db) -> None:
    qname = "rqtest:worker_exec"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 3, 4)
    Worker([q], connection=db).work(burst=True)
    status = job.get_status()
    assert status == JobStatus.FINISHED, f"expected FINISHED, got {status}"
    _cleanup(db, qname, [job.id])


def scenario_worker_stores_result(db) -> None:
    qname = "rqtest:result"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 10, 32)
    Worker([q], connection=db).work(burst=True)
    job.refresh()
    assert job.result == 42, f"expected result 42, got {job.result}"
    _cleanup(db, qname, [job.id])


def scenario_finished_registry(db) -> None:
    qname = "rqtest:fin_reg"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 1, 1)
    Worker([q], connection=db).work(burst=True)
    reg = FinishedJobRegistry(name=qname, connection=db)
    assert len(reg) == 1, f"expected 1 finished job, got {len(reg)}"
    assert reg.contains(job.id), f"job {job.id!r} not in FinishedJobRegistry"
    _cleanup(db, qname, [job.id])


def scenario_failed_registry(db) -> None:
    qname = "rqtest:fail_reg"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_raises)
    Worker([q], connection=db).work(burst=True)
    status = job.get_status()
    assert status == JobStatus.FAILED, f"expected FAILED, got {status}"
    reg = FailedJobRegistry(name=qname, connection=db)
    assert reg.contains(job.id), f"job {job.id!r} not in FailedJobRegistry"
    _cleanup(db, qname, [job.id])


# ─── Job lifecycle scenarios ──────────────────────────────────────────────────

def scenario_job_cancel(db) -> None:
    qname = "rqtest:cancel"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    job = q.enqueue(_add, 9, 9)
    job.cancel()
    assert job.get_status() == JobStatus.CANCELED, "job should be CANCELED"
    # Worker finds an empty queue and exits immediately in burst mode
    Worker([q], connection=db).work(burst=True)
    # Job was not executed — still CANCELED, not in FinishedJobRegistry
    assert job.get_status() == JobStatus.CANCELED, "status changed unexpectedly after worker"
    reg = FinishedJobRegistry(name=qname, connection=db)
    assert not reg.contains(job.id), "canceled job should not be in FinishedJobRegistry"
    _cleanup(db, qname, [job.id])


def scenario_retry_requeues_on_failure(db) -> None:
    qname = "rqtest:retry"
    _cleanup(db, qname)
    q = Queue(qname, connection=db)
    # Retry(max=1): one retry attempt → job is executed twice total, then FAILED
    job = q.enqueue(_raises, retry=Retry(max=1))
    Worker([q], connection=db).work(burst=True)
    status = job.get_status()
    assert status == JobStatus.FAILED, f"expected FAILED after exhausting retries, got {status}"
    reg = FailedJobRegistry(name=qname, connection=db)
    assert reg.contains(job.id), f"job {job.id!r} not in FailedJobRegistry after retries"
    _cleanup(db, qname, [job.id])


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> int:
    db0 = get_client("DB0")
    try:
        # Queue properties
        run("queue_name_and_key", scenario_queue_name_and_key, db0)
        run("queue_length_empty", scenario_queue_length_empty, db0)
        run("queue_len_grows_on_enqueue", scenario_queue_len_grows_on_enqueue, db0)
        run("queue_job_ids", scenario_queue_job_ids, db0)
        # Job creation & metadata
        run("enqueue_returns_queued_job", scenario_enqueue_returns_queued_job, db0)
        run("job_fetch_by_id", scenario_job_fetch_by_id, db0)
        run("job_origin_and_timestamp", scenario_job_origin_and_timestamp, db0)
        run("custom_job_id", scenario_custom_job_id, db0)
        run("at_front_enqueue", scenario_at_front_enqueue, db0)
        # Worker execution
        run("worker_executes_job", scenario_worker_executes_job, db0)
        run("worker_stores_result", scenario_worker_stores_result, db0)
        run("finished_registry", scenario_finished_registry, db0)
        run("failed_registry", scenario_failed_registry, db0)
        # Job lifecycle
        run("job_cancel", scenario_job_cancel, db0)
        run("retry_requeues_on_failure", scenario_retry_requeues_on_failure, db0)
    finally:
        close_all()

    total = _passed + _failed
    print(f"\nSummary: {_passed}/{total} scenarios passed")
    return 0 if _failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
