"""Queue class — RQ-compatible interface over rustycluster lists."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from rustycluster.rq.job import Job
from rustycluster.rq.retry import Retry

if TYPE_CHECKING:
    from rustycluster.client import RustyClusterClient

# Meta-kwargs that Queue.enqueue() intercepts before passing the rest to the fn
_ENQUEUE_META_KEYS = {
    "job_timeout",
    "result_ttl",
    "failure_ttl",
    "retry",
    "job_id",
    "description",
    "at_front",
    "depends_on",
    "on_success",
    "on_failure",
}


class Queue:
    """Distributed job queue backed by a rustycluster list.

    Drop-in replacement for rq.Queue:

        conn = get_client("DB0")          # RustyClusterClient
        q = Queue("my-queue", connection=conn)
        job = q.enqueue(my_function, arg1, kwarg=value)
    """

    def __init__(
        self,
        name: str = "default",
        connection: Optional["RustyClusterClient"] = None,
        default_timeout: int = 180,
        serializer: Any = None,
    ) -> None:
        if connection is None:
            raise ValueError(
                "connection= is required. "
                "Pass a RustyClusterClient: get_client('cluster_name')"
            )
        self._name = name
        self._conn = connection
        self._default_timeout = default_timeout
        # serializer ignored — we always use pickle; kept for API compat

    # ------------------------------------------------------------------
    # RQ-compatible properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def key(self) -> str:
        return f"rc:queue:{self._name}"

    def __len__(self) -> int:
        return self._conn.llen(self.key)

    @property
    def job_ids(self) -> list[str]:
        """All queued job IDs in FIFO order (oldest first)."""
        return self._conn.lrange(self.key, 0, -1)

    # ------------------------------------------------------------------
    # RQ-compatible enqueue
    # ------------------------------------------------------------------

    def enqueue(self, fn: Any, *args: Any, **kwargs: Any) -> Job:
        """Enqueue a job.

        RQ meta-kwargs are intercepted transparently:
            job_timeout, result_ttl, failure_ttl, retry,
            job_id, description, at_front, depends_on

        Everything else is forwarded as positional/keyword args to fn.
        """
        meta: dict[str, Any] = {}
        job_kwargs: dict[str, Any] = {}
        for key, value in kwargs.items():
            if key in _ENQUEUE_META_KEYS:
                meta[key] = value
            else:
                job_kwargs[key] = value

        retry: Optional[Retry] = meta.get("retry")
        retry_max = retry.max if retry else 0
        retry_intervals = retry.interval if retry else [0]

        job = Job._create(
            connection=self._conn,
            fn=fn,
            args=args,
            kwargs=job_kwargs,
            origin=self._name,
            job_id=meta.get("job_id"),
            description=meta.get("description", ""),
            job_timeout=meta.get("job_timeout", self._default_timeout),
            result_ttl=meta.get("result_ttl", 500),
            failure_ttl=meta.get("failure_ttl", -1),
            retry_max=retry_max,
            retry_intervals=retry_intervals,
        )

        if meta.get("at_front"):
            self._conn.lpush(self.key, job.id)
        else:
            self._conn.rpush(self.key, job.id)

        return job

    def enqueue_job(self, job: Job, at_front: bool = False) -> Job:
        """Re-enqueue an existing Job object (used internally by Worker for retries)."""
        if at_front:
            self._conn.lpush(self.key, job.id)
        else:
            self._conn.rpush(self.key, job.id)
        return job
