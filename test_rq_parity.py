"""
test_rq_parity.py

Side-by-side parity comparison: python-rq (Redis) vs rustycluster.rq (RustyCluster).

Runs the SAME test logic against both backends and prints a diff table showing
exactly where behavior diverges.

Requirements:
    Real RQ backend  : pip install rq redis  +  Redis server on localhost:6379
    RustyCluster     : rustycluster.yaml with DB0 pointing to your cluster

Run:
    python test_rq_parity.py
    python test_rq_parity.py --redis-url redis://myhost:6379

If only one backend is reachable, that backend's results are still shown.
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

# ── Backend availability ──────────────────────────────────────────────────────

_RQ_AVAILABLE = False
_RC_AVAILABLE = False
_RQ_ERROR = ""
_RC_ERROR = ""

try:
    import redis as _redis_lib
    from rq import Queue as _RQ_Queue, Retry as _RQ_Retry
    from rq.job import Job as _RQ_Job, JobStatus as _RQ_JobStatus
    from rq.registry import FinishedJobRegistry as _RQ_FinReg
    from rq.registry import FailedJobRegistry as _RQ_FailReg
    try:
        from rq.worker import SimpleWorker as _RQ_Worker  # no fork — closures survive
    except ImportError:
        from rq import Worker as _RQ_Worker
    _RQ_AVAILABLE = True
except ImportError as exc:
    _RQ_ERROR = str(exc)

try:
    from rustycluster import close_all as _rc_close_all
    from rustycluster import get_client as _rc_get_client
    from rustycluster.rq import (
        FailedJobRegistry as _RC_FailReg,
        FinishedJobRegistry as _RC_FinReg,
        Job as _RC_Job,
        JobStatus as _RC_JobStatus,
        Queue as _RC_Queue,
        Retry as _RC_Retry,
        Worker as _RC_Worker,
    )
    _RC_AVAILABLE = True
except ImportError as exc:
    _RC_ERROR = str(exc)


# ── Job callables — imported from a proper module so real RQ workers can resolve them
# (RQ rejects functions defined in __main__)
from rq_parity_jobs import (
    add as _add,
    add_kwargs as _add_kwargs,
    raises as _raises,
    slow as _slow,
)


# ── Backend abstraction ───────────────────────────────────────────────────────

class _Backend:
    name: str

    def make_queue(self, name: str): ...
    def make_worker(self, queues: list): ...
    def make_retry(self, max_retries: int, interval: Any): ...
    def fetch_job(self, job_id: str): ...
    def normalize_status(self, status: Any) -> str: ...
    def finished_registry(self, queue_name: str): ...
    def failed_registry(self, queue_name: str): ...
    def cleanup(self, queue_name: str, job_ids: list[str] | None = None): ...
    def teardown(self): ...


class RealRQBackend(_Backend):
    name = "python-rq (Redis)"

    def __init__(self, redis_url: str):
        self._redis = _redis_lib.from_url(redis_url)

    def make_queue(self, name):
        return _RQ_Queue(name, connection=self._redis)

    def make_worker(self, queues):
        return _RQ_Worker(queues, connection=self._redis)

    def make_retry(self, max_retries, interval):
        return _RQ_Retry(max=max_retries, interval=interval)

    def fetch_job(self, job_id):
        return _RQ_Job.fetch(job_id, connection=self._redis)

    def normalize_status(self, status) -> str:
        v = status.value if hasattr(status, "value") else str(status)
        return v.lower()

    def finished_registry(self, queue_name):
        return _RQ_FinReg(queue_name, connection=self._redis)

    def failed_registry(self, queue_name):
        return _RQ_FailReg(queue_name, connection=self._redis)

    def cleanup(self, queue_name, job_ids=None):
        q = self.make_queue(queue_name)
        try:
            q.empty()
        except Exception:
            pass
        try:
            self._redis.delete(q.key)
        except Exception:
            pass
        for reg in [self.finished_registry(queue_name), self.failed_registry(queue_name)]:
            try:
                self._redis.delete(reg.key)
            except Exception:
                pass
        for jid in (job_ids or []):
            try:
                self._redis.delete(f"rq:job:{jid}")
            except Exception:
                pass

    def teardown(self):
        pass


class RustyClusterRQBackend(_Backend):
    name = "rustycluster.rq"

    def __init__(self):
        self._conn = _rc_get_client("DB0")

    def make_queue(self, name):
        return _RC_Queue(name, connection=self._conn)

    def make_worker(self, queues):
        return _RC_Worker(queues, connection=self._conn)

    def make_retry(self, max_retries, interval):
        return _RC_Retry(max=max_retries, interval=interval)

    def fetch_job(self, job_id):
        return _RC_Job.fetch(job_id, connection=self._conn)

    def normalize_status(self, status) -> str:
        v = status.value if hasattr(status, "value") else str(status)
        return v.lower()

    def finished_registry(self, queue_name):
        return _RC_FinReg(name=queue_name, connection=self._conn)

    def failed_registry(self, queue_name):
        return _RC_FailReg(name=queue_name, connection=self._conn)

    def cleanup(self, queue_name, job_ids=None):
        self._conn.delete(f"rc:queue:{queue_name}")
        self._conn.delete(f"rc:started:{queue_name}")
        self._conn.delete(f"rc:finished:{queue_name}")
        self._conn.delete(f"rc:failed:{queue_name}")
        for jid in (job_ids or []):
            self._conn.delete(f"rc:job:{jid}")

    def teardown(self):
        _rc_close_all()


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass
class _Result:
    ok: bool
    value: Any    # comparable across backends — should be identical when parity holds
    detail: str   # error message if failed


def _safe_run(fn, backend: _Backend) -> _Result:
    try:
        value = fn(backend)
        return _Result(ok=True, value=value, detail="")
    except Exception as exc:
        return _Result(ok=False, value=None, detail=f"{type(exc).__name__}: {exc}")


# ── Test functions ────────────────────────────────────────────────────────────
# Each returns a *comparable* value: the same expected result on both backends
# when parity holds. Diverge tests intentionally return different values on each.

# ── Queue API ─────────────────────────────────────────────────────────────────

def t_queue_len_empty(b: _Backend):
    qname = "parity:len_empty"
    b.cleanup(qname)
    q = b.make_queue(qname)
    result = len(q)
    b.cleanup(qname)
    return result  # 0


def t_queue_len_grows(b: _Backend):
    qname = "parity:len_grow"
    b.cleanup(qname)
    q = b.make_queue(qname)
    j1 = q.enqueue(_add, 1, 2)
    assert len(q) == 1, f"expected 1, got {len(q)}"
    j2 = q.enqueue(_add, 3, 4)
    assert len(q) == 2, f"expected 2, got {len(q)}"
    b.cleanup(qname, [j1.id, j2.id])
    return True


def t_queue_job_ids_fifo(b: _Backend):
    qname = "parity:fifo"
    b.cleanup(qname)
    q = b.make_queue(qname)
    j1 = q.enqueue(_add, 1, 0)
    j2 = q.enqueue(_add, 2, 0)
    ids = q.job_ids
    result = ids.index(j1.id) < ids.index(j2.id)
    b.cleanup(qname, [j1.id, j2.id])
    return result  # True: j1 before j2


def t_queue_at_front(b: _Backend):
    qname = "parity:at_front"
    b.cleanup(qname)
    q = b.make_queue(qname)
    j1 = q.enqueue(_add, 1, 0)
    j2 = q.enqueue(_add, 2, 0, at_front=True)
    ids = q.job_ids
    result = ids[0] == j2.id
    b.cleanup(qname, [j1.id, j2.id])
    return result  # True: j2 pushed to front


def t_queue_enqueue_kwargs(b: _Backend):
    qname = "parity:kwargs"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add_kwargs, 10, offset=5)
    b.make_worker([q]).work(burst=True)
    job.refresh()
    result = job.result
    b.cleanup(qname, [job.id])
    return result  # 15


def t_queue_custom_job_id(b: _Backend):
    qname = "parity:custom_id"
    custom = "parity-custom-job-001"
    b.cleanup(qname, [custom])
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 1, job_id=custom)
    result = job.id == custom
    b.cleanup(qname, [custom])
    return result  # True


# ── Job API ───────────────────────────────────────────────────────────────────

def t_job_status_queued(b: _Backend):
    qname = "parity:status_q"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2)
    status = b.normalize_status(job.get_status())
    b.cleanup(qname, [job.id])
    return status  # "queued"


def t_job_result(b: _Backend):
    qname = "parity:result"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 10, 32)
    b.make_worker([q]).work(burst=True)
    job.refresh()
    result = job.result
    b.cleanup(qname, [job.id])
    return result  # 42


def t_job_status_finished(b: _Backend):
    qname = "parity:status_fin"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2)
    b.make_worker([q]).work(burst=True)
    status = b.normalize_status(job.get_status())
    b.cleanup(qname, [job.id])
    return status  # "finished"


def t_job_status_failed(b: _Backend):
    qname = "parity:status_fail"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_raises)
    b.make_worker([q]).work(burst=True)
    status = b.normalize_status(job.get_status())
    b.cleanup(qname, [job.id])
    return status  # "failed"


def t_job_origin(b: _Backend):
    qname = "parity:origin"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2)
    result = job.origin
    b.cleanup(qname, [job.id])
    return result  # "parity:origin"


def t_job_enqueued_at_is_datetime(b: _Backend):
    qname = "parity:enqueued_at"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2)
    result = isinstance(job.enqueued_at, datetime)
    b.cleanup(qname, [job.id])
    return result  # True


def t_job_started_at_after_work(b: _Backend):
    qname = "parity:started_at"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2)
    b.make_worker([q]).work(burst=True)
    job.refresh()
    result = isinstance(job.started_at, datetime)
    b.cleanup(qname, [job.id])
    return result  # True


def t_job_exc_info_on_failure(b: _Backend):
    qname = "parity:exc_info"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_raises)
    b.make_worker([q]).work(burst=True)
    job.refresh()
    result = "ValueError" in (job.exc_info or "")
    b.cleanup(qname, [job.id])
    return result  # True


def t_job_cancel_status(b: _Backend):
    qname = "parity:cancel"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2)
    job.cancel()
    status = b.normalize_status(job.get_status())
    b.cleanup(qname, [job.id])
    return status  # "canceled"


def t_job_cancel_removes_from_queue(b: _Backend):
    qname = "parity:cancel_rm"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2)
    job.cancel()
    result = job.id not in q.job_ids
    b.cleanup(qname, [job.id])
    return result  # True


def t_job_fetch(b: _Backend):
    qname = "parity:fetch"
    b.cleanup(qname)
    q = b.make_queue(qname)
    original = q.enqueue(_add, 5, 6)
    fetched = b.fetch_job(original.id)
    result = (
        fetched.id == original.id
        and b.normalize_status(fetched.get_status()) == "queued"
    )
    b.cleanup(qname, [original.id])
    return result  # True


def t_job_refresh(b: _Backend):
    qname = "parity:refresh"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 3, 4)
    b.make_worker([q]).work(burst=True)
    job.refresh()
    result = job.result
    b.cleanup(qname, [job.id])
    return result  # 7


def t_job_timeout_enforcement(b: _Backend):
    qname = "parity:timeout"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_slow, 10, job_timeout=1)
    b.make_worker([q]).work(burst=True)
    job.refresh()
    status = b.normalize_status(job.get_status())
    b.cleanup(qname, [job.id])
    return status  # "failed"


# ── Worker API ────────────────────────────────────────────────────────────────

def t_worker_burst_returns_bool(b: _Backend):
    qname = "parity:burst"
    b.cleanup(qname)
    q = b.make_queue(qname)
    q.enqueue(_add, 1, 2)
    result = b.make_worker([q]).work(burst=True)
    b.cleanup(qname)
    return bool(result)  # True


def t_worker_multi_queue(b: _Backend):
    q1, q2 = "parity:mq1", "parity:mq2"
    b.cleanup(q1)
    b.cleanup(q2)
    queue1 = b.make_queue(q1)
    queue2 = b.make_queue(q2)
    j1 = queue1.enqueue(_add, 1, 0)
    j2 = queue2.enqueue(_add, 2, 0)
    b.make_worker([queue1, queue2]).work(burst=True)
    result = (
        b.normalize_status(j1.get_status()) == "finished"
        and b.normalize_status(j2.get_status()) == "finished"
    )
    b.cleanup(q1, [j1.id])
    b.cleanup(q2, [j2.id])
    return result  # True


# ── Retry ─────────────────────────────────────────────────────────────────────

def t_retry_max_attr(b: _Backend):
    r = b.make_retry(3, [0, 5])
    return r.max  # 3


def t_retry_interval_attr(b: _Backend):
    r = b.make_retry(3, [0, 5, 30])
    # rq uses .intervals (plural), rustycluster uses .interval (singular)
    intervals = getattr(r, "interval", None) or getattr(r, "intervals", None)
    assert intervals == [0, 5, 30], f"got {intervals}"
    return intervals  # [0, 5, 30]


def t_retry_on_failure(b: _Backend):
    qname = "parity:retry"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_raises, retry=b.make_retry(1, [0]))
    b.make_worker([q]).work(burst=True)
    status = b.normalize_status(job.get_status())
    b.cleanup(qname, [job.id])
    return status  # "failed"


# ── Registries ────────────────────────────────────────────────────────────────

def t_finished_registry(b: _Backend):
    qname = "parity:reg_fin"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 1)
    b.make_worker([q]).work(burst=True)
    reg = b.finished_registry(qname)
    # use get_job_ids() — common to both rq and rustycluster.rq
    result = len(reg) == 1 and job.id in reg.get_job_ids()
    b.cleanup(qname, [job.id])
    return result  # True


def t_failed_registry(b: _Backend):
    qname = "parity:reg_fail"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_raises)
    b.make_worker([q]).work(burst=True)
    reg = b.failed_registry(qname)
    result = len(reg) == 1 and job.id in reg.get_job_ids()
    b.cleanup(qname, [job.id])
    return result  # True


# ── Divergence tests — these intentionally surface gaps ───────────────────────

def t_queue_empty_method(b: _Backend):
    """rq: Queue.empty() flushes all jobs → True | rc: AttributeError"""
    qname = "parity:empty"
    b.cleanup(qname)
    q = b.make_queue(qname)
    j = q.enqueue(_add, 1, 2)
    q.empty()
    result = len(q) == 0
    b.cleanup(qname, [j.id])
    return result  # rq: True | rc: AttributeError


def t_queue_count_property(b: _Backend):
    """rq: q.count alias for len | rc: AttributeError"""
    qname = "parity:count"
    b.cleanup(qname)
    q = b.make_queue(qname)
    j = q.enqueue(_add, 1, 2)
    result = q.count == 1  # rq: True | rc: AttributeError
    b.cleanup(qname, [j.id])
    return result


def t_job_description_property(b: _Backend):
    """rq: job.description is a public str | rc: AttributeError (_description private)"""
    qname = "parity:desc"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2, description="my job desc")
    result = isinstance(job.description, str) and "desc" in job.description
    b.cleanup(qname, [job.id])
    return result  # rq: True | rc: AttributeError


def t_job_meta_dict(b: _Backend):
    """rq: job.meta is a dict | rc: AttributeError (not implemented)"""
    qname = "parity:meta"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2)
    result = isinstance(job.meta, dict)
    b.cleanup(qname, [job.id])
    return result  # rq: True | rc: AttributeError


def t_job_delete_public(b: _Backend):
    """rq: job.delete() is public | rc: AttributeError (only _delete() private)"""
    qname = "parity:delete"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2)
    job.delete()
    b.cleanup(qname)
    return True  # rq: True | rc: AttributeError


def t_job_timeout_property(b: _Backend):
    """rq: job.timeout is public int | rc: AttributeError (_job_timeout private)"""
    qname = "parity:timeout_prop"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 2, job_timeout=60)
    result = job.timeout == 60
    b.cleanup(qname, [job.id])
    return result  # rq: True | rc: AttributeError


def t_on_success_callback(b: _Backend):
    """rq: on_success callback is invoked | rc: silently ignored"""
    called: list = []

    def success_cb(job, conn, result, *a, **kw):
        called.append(result)

    qname = "parity:on_success"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 1, on_success=success_cb)
    b.make_worker([q]).work(burst=True)
    result = len(called) > 0
    b.cleanup(qname, [job.id])
    return result  # rq: True | rc: False


def t_on_failure_callback(b: _Backend):
    """rq: on_failure callback is invoked | rc: silently ignored"""
    called: list = []

    def failure_cb(job, conn, typ, val, tb):
        called.append(True)

    qname = "parity:on_failure"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_raises, on_failure=failure_cb)
    b.make_worker([q]).work(burst=True)
    result = len(called) > 0
    b.cleanup(qname, [job.id])
    return result  # rq: True | rc: False


def t_depends_on_defers_job(b: _Backend):
    """rq: depends_on puts j2 in DEFERRED | rc: silently ignored, j2 is QUEUED"""
    qname = "parity:depends"
    b.cleanup(qname)
    q = b.make_queue(qname)
    j1 = q.enqueue(_add, 1, 0)
    j2 = q.enqueue(_add, 2, 0, depends_on=j1)
    status_j2 = b.normalize_status(j2.get_status())
    b.cleanup(qname, [j1.id, j2.id])
    return status_j2  # rq: "deferred" | rc: "queued"


def t_result_ttl_enforced(b: _Backend):
    """rq: result_ttl=1 expires job hash after 1s | rc: hash persists (not enforced)"""
    qname = "parity:result_ttl"
    b.cleanup(qname)
    q = b.make_queue(qname)
    job = q.enqueue(_add, 1, 1, result_ttl=1)
    b.make_worker([q]).work(burst=True)
    time.sleep(2)
    try:
        b.fetch_job(job.id)
        return "not_expired"   # rc: hash persists
    except Exception:
        return "expired"       # rq: hash gone after TTL


# ── Test registry ─────────────────────────────────────────────────────────────

TESTS: list[tuple[str, Any]] = [
    # Queue API
    ("queue: len when empty",                   t_queue_len_empty),
    ("queue: len grows on enqueue",             t_queue_len_grows),
    ("queue: job_ids FIFO order",               t_queue_job_ids_fifo),
    ("queue: at_front=True",                    t_queue_at_front),
    ("queue: enqueue with kwargs",              t_queue_enqueue_kwargs),
    ("queue: custom job_id",                    t_queue_custom_job_id),
    # Job API
    ("job: get_status() → queued",              t_job_status_queued),
    ("job: result after work",                  t_job_result),
    ("job: get_status() → finished",            t_job_status_finished),
    ("job: get_status() → failed",              t_job_status_failed),
    ("job: .origin property",                   t_job_origin),
    ("job: .enqueued_at is datetime",           t_job_enqueued_at_is_datetime),
    ("job: .started_at set after work",         t_job_started_at_after_work),
    ("job: .exc_info contains traceback",       t_job_exc_info_on_failure),
    ("job: cancel() → canceled status",         t_job_cancel_status),
    ("job: cancel() removes from queue",        t_job_cancel_removes_from_queue),
    ("job: Job.fetch(id, conn)",                t_job_fetch),
    ("job: refresh() reloads result",           t_job_refresh),
    ("job: job_timeout enforcement",            t_job_timeout_enforcement),
    # Worker API
    ("worker: work(burst=True) returns bool",   t_worker_burst_returns_bool),
    ("worker: multi-queue polling",             t_worker_multi_queue),
    # Retry
    ("retry: Retry.max attribute",              t_retry_max_attr),
    ("retry: Retry interval list",              t_retry_interval_attr),
    ("retry: exhaustion → failed status",       t_retry_on_failure),
    # Registries
    ("registry: FinishedJobRegistry",           t_finished_registry),
    ("registry: FailedJobRegistry",             t_failed_registry),
    # ── Divergence tests ──────────────────────────────────────────────────────
    ("DIVERGE: queue.empty() method",           t_queue_empty_method),
    ("DIVERGE: queue.count property",           t_queue_count_property),
    ("DIVERGE: job.description property",       t_job_description_property),
    ("DIVERGE: job.meta dict",                  t_job_meta_dict),
    ("DIVERGE: job.delete() public method",     t_job_delete_public),
    ("DIVERGE: job.timeout property",           t_job_timeout_property),
    ("DIVERGE: on_success callback invoked",    t_on_success_callback),
    ("DIVERGE: on_failure callback invoked",    t_on_failure_callback),
    ("DIVERGE: depends_on defers job",          t_depends_on_defers_job),
    ("DIVERGE: result_ttl expires key",         t_result_ttl_enforced),
]


# ── Comparison & output ───────────────────────────────────────────────────────

def _verdict(rq_r: Optional[_Result], rc_r: Optional[_Result]) -> str:
    if rq_r is None and rc_r is None:
        return "N/A"
    if rq_r is None:
        return "RC_ONLY"
    if rc_r is None:
        return "RQ_ONLY"
    if not rq_r.ok and not rc_r.ok:
        return "BOTH_FAIL"
    if rq_r.ok != rc_r.ok:
        return "DIVERGE"
    if rq_r.value != rc_r.value:
        return "DIVERGE"
    return "MATCH"


_VERDICT_LABEL = {
    "MATCH":     "✓ MATCH",
    "DIVERGE":   "⚠ DIVERGE",
    "BOTH_FAIL": "✗ BOTH_FAIL",
    "RQ_ONLY":   "~ RQ_ONLY",
    "RC_ONLY":   "~ RC_ONLY",
    "N/A":       "- N/A",
}


def _fmt(r: Optional[_Result], width: int) -> str:
    if r is None:
        return "N/A".ljust(width)
    if r.ok:
        v = repr(r.value)
        label = f"PASS {v}" if len(v) <= width - 6 else f"PASS {v[:width-9]}..."
    else:
        d = r.detail
        label = f"FAIL {d}" if len(d) <= width - 6 else f"FAIL {d[:width-9]}..."
    return label.ljust(width)


def main() -> int:
    parser = argparse.ArgumentParser(description="python-rq vs rustycluster.rq parity")
    parser.add_argument("--redis-url", default="redis://localhost:6379",
                        help="Redis URL for the python-rq backend (default: redis://localhost:6379)")
    args = parser.parse_args()

    print("\n" + "═" * 82)
    print("  python-rq  vs  rustycluster.rq  —  Side-by-Side Parity Comparison")
    print("═" * 82)

    rq_backend: Optional[_Backend] = None
    rc_backend: Optional[_Backend] = None

    # ── Backend setup ─────────────────────────────────────────────────────────
    if _RQ_AVAILABLE:
        try:
            b = RealRQBackend(redis_url=args.redis_url)
            b._redis.ping()
            rq_backend = b
            print(f"  [✓] python-rq   ready  ({args.redis_url})")
        except Exception as exc:
            print(f"  [✗] python-rq   unavailable — Redis not reachable: {exc}")
    else:
        print(f"  [✗] python-rq   not installed ({_RQ_ERROR})")

    if _RC_AVAILABLE:
        try:
            b = RustyClusterRQBackend()
            b._conn.ping()
            rc_backend = b
            print(f"  [✓] rustycluster.rq  ready  (DB0)")
        except Exception as exc:
            print(f"  [✗] rustycluster.rq  unavailable — cluster not reachable: {exc}")
    else:
        print(f"  [✗] rustycluster.rq  not installed ({_RC_ERROR})")

    if rq_backend is None and rc_backend is None:
        print("\nNo backends available. Exiting.")
        return 1

    print()

    # ── Table header ──────────────────────────────────────────────────────────
    W_NAME, W_RQ, W_RC, W_VER = 42, 26, 26, 14
    sep = "─" * (W_NAME + W_RQ + W_RC + W_VER + 3)

    rq_label = (rq_backend.name if rq_backend else "python-rq (N/A)").ljust(W_RQ)
    rc_label = (rc_backend.name if rc_backend else "rustycluster.rq (N/A)").ljust(W_RC)
    print(f"{'Test':<{W_NAME}} {rq_label} {rc_label} {'Verdict'}")
    print(sep)

    comparisons: list[tuple[str, Optional[_Result], Optional[_Result], str]] = []
    prev_section = ""

    for name, fn in TESTS:
        section = name.split(":")[0]
        if section != prev_section:
            if prev_section:
                print()
            prev_section = section

        rq_r = _safe_run(fn, rq_backend) if rq_backend else None
        rc_r = _safe_run(fn, rc_backend) if rc_backend else None
        v = _verdict(rq_r, rc_r)
        comparisons.append((name, rq_r, rc_r, v))

        print(
            f"{name:<{W_NAME}} "
            f"{_fmt(rq_r, W_RQ)} "
            f"{_fmt(rc_r, W_RC)} "
            f"{_VERDICT_LABEL.get(v, v)}"
        )

    print(sep)

    # ── Summary ───────────────────────────────────────────────────────────────
    counts = {k: 0 for k in _VERDICT_LABEL}
    for _, _, _, v in comparisons:
        counts[v] = counts.get(v, 0) + 1

    total = len(comparisons)
    print(f"\n  Total tests : {total}")
    print(f"  ✓ MATCH     : {counts['MATCH']}")
    print(f"  ⚠ DIVERGE   : {counts['DIVERGE']}")
    if counts["BOTH_FAIL"]:
        print(f"  ✗ BOTH_FAIL : {counts['BOTH_FAIL']}")
    if counts["RQ_ONLY"]:
        print(f"  ~ RQ_ONLY   : {counts['RQ_ONLY']}")
    if counts["RC_ONLY"]:
        print(f"  ~ RC_ONLY   : {counts['RC_ONLY']}")

    # ── Divergence detail ─────────────────────────────────────────────────────
    divergences = [(n, rq_r, rc_r) for n, rq_r, rc_r, v in comparisons if v == "DIVERGE"]
    if divergences:
        print("\n── Divergence detail ──────────────────────────────────────────────────────")
        for name, rq_r, rc_r in divergences:
            rq_info = (
                f"PASS → {rq_r.value!r}" if (rq_r and rq_r.ok)
                else f"FAIL → {rq_r.detail}" if rq_r
                else "N/A"
            )
            rc_info = (
                f"PASS → {rc_r.value!r}" if (rc_r and rc_r.ok)
                else f"FAIL → {rc_r.detail}" if rc_r
                else "N/A"
            )
            print(f"\n  ⚠ {name}")
            print(f"      python-rq        : {rq_info}")
            print(f"      rustycluster.rq  : {rc_info}")

    if rc_backend:
        rc_backend.teardown()

    return 0


if __name__ == "__main__":
    sys.exit(main())
