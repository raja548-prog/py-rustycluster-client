"""Unit tests for rustycluster.rq — mocks the RustyClusterClient so no server is needed."""

from __future__ import annotations

import base64
import pickle
from unittest.mock import MagicMock, call, patch

import pytest

from rustycluster.rq import (
    FailedJobRegistry,
    FinishedJobRegistry,
    Job,
    JobStatus,
    Queue,
    Retry,
    StartedJobRegistry,
    Worker,
    get_current_job,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn():
    """Return a fully mocked RustyClusterClient."""
    conn = MagicMock()
    conn.hget_all.return_value = {}
    conn.hmset.return_value = True
    conn.hset.return_value = True
    conn.rpush.return_value = 1
    conn.lpush.return_value = 1
    conn.lrange.return_value = []
    conn.llen.return_value = 0
    conn.lrem.return_value = 1
    conn.zadd.return_value = 1
    conn.zrem.return_value = 1
    conn.zcard.return_value = 0
    conn.zrange_by_score.return_value = []
    conn.delete.return_value = True
    return conn


def _ser(obj) -> str:
    return base64.b64encode(pickle.dumps(obj)).decode("ascii")


def _sample_job_data(job_id: str, queue_name: str = "default", **overrides) -> dict:
    data = {
        "id": job_id,
        "status": JobStatus.QUEUED.value,
        "func_name": "builtins:len",
        "args_b64": _ser(([1, 2, 3],)),
        "kwargs_b64": _ser({}),
        "result_b64": "",
        "exc_info": "",
        "origin": queue_name,
        "enqueued_at": "2026-01-01T00:00:00+00:00",
        "started_at": "",
        "ended_at": "",
        "retries_left": "0",
        "retries_total": "0",
        "retry_intervals": "0",
        "description": "len",
        "job_timeout": "180",
        "result_ttl": "500",
        "failure_ttl": "-1",
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------

class TestRetry:
    def test_defaults(self):
        r = Retry(max=3)
        assert r.max == 3
        assert r.interval == [0]

    def test_interval_int(self):
        r = Retry(max=2, interval=5)
        assert r.interval == [5]

    def test_interval_list(self):
        r = Retry(max=3, interval=[10, 20, 30])
        assert r.interval == [10, 20, 30]

    def test_get_interval_clamps_to_last(self):
        r = Retry(max=5, interval=[1, 2])
        assert r.get_interval(0) == 1
        assert r.get_interval(1) == 2
        assert r.get_interval(4) == 2  # repeats last


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------

class TestQueue:
    def test_name_and_key(self):
        conn = _make_conn()
        q = Queue("emails", connection=conn)
        assert q.name == "emails"
        assert q.key == "rc:queue:emails"

    def test_len_delegates_to_llen(self):
        conn = _make_conn()
        conn.llen.return_value = 7
        q = Queue("emails", connection=conn)
        assert len(q) == 7
        conn.llen.assert_called_once_with("rc:queue:emails")

    def test_job_ids_delegates_to_lrange(self):
        conn = _make_conn()
        conn.lrange.return_value = ["id1", "id2"]
        q = Queue("emails", connection=conn)
        assert q.job_ids == ["id1", "id2"]
        conn.lrange.assert_called_once_with("rc:queue:emails", 0, -1)

    def test_enqueue_saves_job_and_rpushes(self):
        conn = _make_conn()
        q = Queue("emails", connection=conn)
        job = q.enqueue(len, [1, 2, 3])

        assert isinstance(job, Job)
        assert job.origin == "emails"
        assert job._status == JobStatus.QUEUED

        # hmset should have been called to persist the job hash
        conn.hmset.assert_called()
        saved_data = conn.hmset.call_args[0][1]
        assert saved_data["func_name"] == "builtins:len"

        # rpush adds the job ID to the queue list
        conn.rpush.assert_called_once_with("rc:queue:emails", job.id)

    def test_enqueue_at_front_uses_lpush(self):
        conn = _make_conn()
        q = Queue("default", connection=conn)
        job = q.enqueue(len, [1], at_front=True)
        conn.lpush.assert_called_once_with("rc:queue:default", job.id)

    def test_enqueue_strips_meta_kwargs(self):
        conn = _make_conn()
        q = Queue("default", connection=conn)

        def fn(x, y=1):
            return x + y

        job = q.enqueue(fn, 10, y=5, job_timeout=60, result_ttl=300, retry=Retry(max=2))
        assert job._job_timeout == 60
        assert job._result_ttl == 300
        assert job._retries_left == 2

        # Only x=10 and y=5 should reach fn
        saved_data = conn.hmset.call_args[0][1]
        kwargs = pickle.loads(base64.b64decode(saved_data["kwargs_b64"]))
        assert kwargs == {"y": 5}

    def test_missing_connection_raises(self):
        with pytest.raises(ValueError, match="connection="):
            Queue("default")


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

class TestJob:
    def test_fetch_loads_fields(self):
        conn = _make_conn()
        job_id = "abc-123"
        conn.hget_all.return_value = _sample_job_data(job_id)
        job = Job.fetch(job_id, conn)

        assert job.id == job_id
        assert job.origin == "default"
        assert job._func_name == "builtins:len"

    def test_fetch_missing_raises(self):
        conn = _make_conn()
        conn.hget_all.return_value = {}
        with pytest.raises(ValueError, match="not found"):
            Job.fetch("ghost-id", conn)

    def test_get_status_reads_live(self):
        conn = _make_conn()
        job_id = "abc-123"
        conn.hget_all.return_value = _sample_job_data(job_id, status="started")
        job = Job.fetch(job_id, conn)

        conn.hget_all.return_value = _sample_job_data(job_id, status="finished")
        assert job.get_status() == JobStatus.FINISHED

    def test_cancel_sets_status_and_removes_from_queue(self):
        conn = _make_conn()
        job_id = "abc-123"
        conn.hget_all.return_value = _sample_job_data(job_id, origin="myqueue")
        job = Job.fetch(job_id, conn)
        job.cancel()

        conn.hset.assert_called_with("rc:job:abc-123", "status", "canceled")
        conn.lrem.assert_called_with("rc:queue:myqueue", 0, job_id)

    def test_result_unpickles(self):
        conn = _make_conn()
        job_id = "abc-123"
        conn.hget_all.return_value = _sample_job_data(
            job_id, result_b64=_ser(42)
        )
        job = Job.fetch(job_id, conn)
        assert job.result == 42


# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------

class TestRegistries:
    def test_key_format(self):
        conn = _make_conn()
        q = Queue("myqueue", connection=conn)
        assert StartedJobRegistry(queue=q).key == "rc:started:myqueue"
        assert FinishedJobRegistry(queue=q).key == "rc:finished:myqueue"
        assert FailedJobRegistry(queue=q).key == "rc:failed:myqueue"

    def test_get_job_ids_delegates_to_zrange_by_score(self):
        conn = _make_conn()
        conn.zrange_by_score.return_value = ["id1", "id2"]
        reg = StartedJobRegistry(name="myqueue", connection=conn)
        ids = reg.get_job_ids()
        assert ids == ["id1", "id2"]
        conn.zrange_by_score.assert_called_once_with("rc:started:myqueue", 0, float("inf"))

    def test_len_delegates_to_zcard(self):
        conn = _make_conn()
        conn.zcard.return_value = 5
        reg = FailedJobRegistry(name="myqueue", connection=conn)
        assert len(reg) == 5

    def test_missing_args_raises(self):
        conn = _make_conn()
        with pytest.raises(ValueError):
            StartedJobRegistry()

    def test_name_and_connection_args(self):
        conn = _make_conn()
        reg = FinishedJobRegistry(name="test", connection=conn)
        assert reg.key == "rc:finished:test"


# ---------------------------------------------------------------------------
# Worker — success path
# ---------------------------------------------------------------------------

class TestWorkerSuccess:
    def test_executes_job_and_marks_finished(self):
        conn = _make_conn()
        job_id = "job-ok"

        # blpop returns one job then None (burst exits on empty)
        conn.blpop.side_effect = [
            ("rc:queue:default", job_id),
            None,
        ]
        conn.hget_all.return_value = _sample_job_data(
            job_id,
            func_name="builtins:len",
            args_b64=_ser(([1, 2, 3],)),
        )

        q = Queue("default", connection=conn)
        w = Worker([q], connection=conn)
        w.work(burst=True)

        # hmset should have been called with status=finished
        calls_data = [c[0][1] for c in conn.hmset.call_args_list]
        statuses = [d.get("status") for d in calls_data]
        assert "finished" in statuses

        # result should be pickled 3 (len([1,2,3]) == 3)
        finished_data = next(d for d in calls_data if d.get("status") == "finished")
        assert pickle.loads(base64.b64decode(finished_data["result_b64"])) == 3

    def test_sets_current_job_during_execution(self):
        conn = _make_conn()
        job_id = "job-current"
        captured = []

        def my_fn():
            captured.append(get_current_job())

        import sys
        sys.modules.setdefault("_test_worker_helpers", MagicMock())

        # Patch _ref_to_func so we can return our closure
        with patch("rustycluster.rq.job._ref_to_func", return_value=my_fn):
            conn.blpop.side_effect = [("rc:queue:default", job_id), None]
            conn.hget_all.return_value = _sample_job_data(
                job_id,
                func_name="_test_worker_helpers:my_fn",
                args_b64=_ser(()),
            )
            q = Queue("default", connection=conn)
            w = Worker([q], connection=conn)
            w.work(burst=True)

        assert len(captured) == 1
        assert captured[0] is not None
        assert captured[0].id == job_id

    def test_current_job_cleared_after_execution(self):
        conn = _make_conn()
        conn.blpop.return_value = None
        q = Queue("default", connection=conn)
        w = Worker([q], connection=conn)
        w.work(burst=True)
        assert get_current_job() is None


# ---------------------------------------------------------------------------
# Worker — failure and retry paths
# ---------------------------------------------------------------------------

class TestWorkerFailure:
    def _boom(self):
        raise ValueError("boom")

    def test_failed_job_moves_to_failed_registry(self):
        conn = _make_conn()
        job_id = "job-fail"

        with patch("rustycluster.rq.job._ref_to_func", side_effect=Exception("no such fn")):
            conn.blpop.side_effect = [("rc:queue:default", job_id), None]
            conn.hget_all.return_value = _sample_job_data(job_id)
            q = Queue("default", connection=conn)
            w = Worker([q], connection=conn)
            w.work(burst=True)

        calls_data = [c[0][1] for c in conn.hmset.call_args_list]
        statuses = [d.get("status") for d in calls_data]
        assert "failed" in statuses

        # zadd called for failed registry
        zadd_keys = [c[0][0] for c in conn.zadd.call_args_list]
        assert any("rc:failed:default" in k for k in zadd_keys)

    def test_retry_re_enqueues_with_decremented_count(self):
        conn = _make_conn()
        job_id = "job-retry"

        with patch("rustycluster.rq.job._ref_to_func", side_effect=Exception("transient")):
            conn.blpop.side_effect = [("rc:queue:default", job_id), None]
            conn.hget_all.return_value = _sample_job_data(
                job_id,
                retries_left="2",
                retries_total="2",
                retry_intervals="0",
            )
            q = Queue("default", connection=conn)
            w = Worker([q], connection=conn)
            w.work(burst=True)

        # Job should have been re-pushed to the queue (rpush for re-enqueue)
        rpush_calls = conn.rpush.call_args_list
        assert any("rc:queue:default" in str(c) for c in rpush_calls)

        # retries_left should have decremented to 1
        calls_data = [c[0][1] for c in conn.hmset.call_args_list]
        retry_left_values = [d.get("retries_left") for d in calls_data if "retries_left" in d]
        assert "1" in retry_left_values


# ---------------------------------------------------------------------------
# get_current_job() outside worker
# ---------------------------------------------------------------------------

def test_get_current_job_returns_none_outside_worker():
    import rustycluster.rq.worker as wmod
    # Clear any leftover state
    wmod._thread_local.current_job = None
    assert get_current_job() is None
