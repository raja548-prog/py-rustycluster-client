"""Worker class and get_current_job() — RQ-compatible interface over rustycluster."""

from __future__ import annotations

import logging
import signal
import threading
import time
import traceback
from typing import TYPE_CHECKING, Any, Optional

from rustycluster.rq.job import Job, JobStatus, _now_iso
from rustycluster.rq.registries import FailedJobRegistry, FinishedJobRegistry, StartedJobRegistry

if TYPE_CHECKING:
    from rustycluster.client import RustyClusterClient
    from rustycluster.rq.queue import Queue

logger = logging.getLogger("rustycluster.rq.worker")

# Thread-local storage for get_current_job()
_thread_local = threading.local()


def get_current_job() -> Optional[Job]:
    """Return the Job currently being executed in this thread, or None."""
    return getattr(_thread_local, "current_job", None)


class Worker:
    """Job worker backed by rustycluster.

    Drop-in replacement for rq.Worker:

        conn = get_client("DB0")
        w = Worker([q1, q2], connection=conn)
        w.work(burst=True)   # process all queued jobs and exit

    The worker polls queues in order using blpop (blocking left-pop),
    exactly mirroring RQ's round-robin queue priority model.
    """

    # How long to block on blpop each iteration (seconds).
    # A short timeout lets the burst-mode exit check run frequently.
    _BLPOP_TIMEOUT: float = 2.0

    def __init__(
        self,
        queues: list["Queue"],
        connection: Optional["RustyClusterClient"] = None,
    ) -> None:
        if not queues:
            raise ValueError("queues must be a non-empty list")
        self._queues = queues
        # Use the connection from the first queue if not provided directly
        self._conn = connection or queues[0]._conn
        self._running = False
        self._shutdown_requested = False

    # ------------------------------------------------------------------
    # RQ-compatible public interface
    # ------------------------------------------------------------------

    def work(self, burst: bool = False, with_scheduler: bool = False) -> bool:
        """Start the worker loop.

        Args:
            burst: When True, exit after all queues are empty.
            with_scheduler: Accepted for API compatibility; not used.

        Returns:
            True if at least one job was processed.
        """
        self._running = True
        self._shutdown_requested = False
        processed = False
        queue_keys = [q.key for q in self._queues]

        self._install_signal_handlers()

        logger.info(
            "Worker started. Queues: %s | burst=%s",
            [q.name for q in self._queues],
            burst,
        )

        try:
            while not self._shutdown_requested:
                result = self._conn.blpop(*queue_keys, timeout=self._BLPOP_TIMEOUT)

                if result is None:
                    if burst:
                        break
                    continue

                queue_key, job_id = result
                self._execute_job(job_id, queue_key)
                processed = True
        finally:
            self._running = False
            logger.info("Worker stopped.")

        return processed

    # ------------------------------------------------------------------
    # Job execution
    # ------------------------------------------------------------------

    def _execute_job(self, job_id: str, queue_key: str) -> None:
        try:
            job = Job.fetch(job_id, self._conn)
        except Exception:
            logger.error("Could not fetch job %s — skipping.", job_id)
            return

        queue_name = job.origin or queue_key.removeprefix("rc:queue:")
        started_reg = StartedJobRegistry(name=queue_name, connection=self._conn)
        finished_reg = FinishedJobRegistry(name=queue_name, connection=self._conn)
        failed_reg = FailedJobRegistry(name=queue_name, connection=self._conn)

        # Mark as started
        job._status = JobStatus.STARTED
        job._started_at = _now_iso()
        job._save()
        started_reg.add(job.id)
        _thread_local.current_job = job

        logger.info("Executing job %s (%s)", job.id, job._func_name)

        try:
            fn = job._get_callable()
            args = job._get_args()
            kwargs = job._get_kwargs()

            if job._job_timeout > 0:
                result = self._run_with_timeout(fn, args, kwargs, job._job_timeout, job)
            else:
                result = fn(*args, **kwargs)

            # Success
            import base64
            import pickle
            job._result_b64 = base64.b64encode(pickle.dumps(result)).decode("ascii")
            job._status = JobStatus.FINISHED
            job._ended_at = _now_iso()
            job._save()

            started_reg.remove(job.id)
            finished_reg.add(job.id)
            logger.info("Job %s finished successfully.", job.id)

        except Exception:
            exc_text = traceback.format_exc()
            job._exc_info = exc_text
            job._ended_at = _now_iso()
            logger.error("Job %s failed:\n%s", job.id, exc_text)

            if job._retries_left > 0:
                self._retry_job(job, started_reg)
            else:
                job._status = JobStatus.FAILED
                job._save()
                started_reg.remove(job.id)
                failed_reg.add(job.id)
                logger.warning("Job %s moved to FailedJobRegistry.", job.id)

        finally:
            _thread_local.current_job = None

    def _retry_job(self, job: Job, started_reg: StartedJobRegistry) -> None:
        intervals = [int(x) for x in job._retry_intervals.split(",")]
        # attempt is 0-based: first retry = 0, second = 1, ...
        attempt = job._retries_total - job._retries_left
        idx = min(attempt, len(intervals) - 1)
        delay = intervals[idx]

        job._retries_left -= 1
        job._status = JobStatus.QUEUED
        job._started_at = None
        job._ended_at = None
        job._result_b64 = ""
        job._enqueued_at = _now_iso()
        job._save()

        started_reg.remove(job.id)

        if delay > 0:
            time.sleep(delay)

        from rustycluster.rq.queue import Queue

        q = Queue(job.origin, connection=self._conn)
        q.enqueue_job(job)
        logger.info(
            "Job %s re-enqueued for retry (%d retries left).",
            job.id,
            job._retries_left,
        )

    @staticmethod
    def _run_with_timeout(fn: Any, args: tuple, kwargs: dict, timeout: int, job: Any = None) -> Any:
        """Execute fn in a thread with a wall-clock timeout."""
        result_box: list = []
        exc_box: list = []

        def target():
            if job is not None:
                _thread_local.current_job = job
            try:
                result_box.append(fn(*args, **kwargs))
            except Exception as exc:
                exc_box.append(exc)
            finally:
                _thread_local.current_job = None

        t = threading.Thread(target=target, daemon=True)
        t.start()
        t.join(timeout)
        if t.is_alive():
            raise TimeoutError(f"Job exceeded timeout of {timeout}s")
        if exc_box:
            raise exc_box[0]
        return result_box[0] if result_box else None

    # ------------------------------------------------------------------
    # Signal handling (graceful shutdown on Ctrl-C / SIGTERM)
    # ------------------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        try:
            signal.signal(signal.SIGINT, self._handle_stop_signal)
            signal.signal(signal.SIGTERM, self._handle_stop_signal)
        except (OSError, ValueError):
            # Not the main thread or signal not supported on this platform
            pass

    def _handle_stop_signal(self, signum: int, frame: Any) -> None:
        logger.info("Stop signal received — finishing current job then exiting.")
        self._shutdown_requested = True


