"""Job class and JobStatus — RQ-compatible interface over rustycluster."""

from __future__ import annotations

import base64
import pickle
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from rustycluster.client import RustyClusterClient

# Prefix for all job hashes in the cluster
_JOB_KEY_PREFIX = "rc:job:"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _serialize(obj: Any) -> str:
    return base64.b64encode(pickle.dumps(obj)).decode("ascii")


def _deserialize(value: str | None) -> Any:
    if not value:
        return None
    return pickle.loads(base64.b64decode(value))


def _func_to_ref(fn: Any) -> str:
    return f"{fn.__module__}:{fn.__qualname__}"


def _ref_to_func(ref: str) -> Any:
    import importlib

    module_path, qualname = ref.rsplit(":", 1)
    module = importlib.import_module(module_path)
    obj = module
    for attr in qualname.split("."):
        obj = getattr(obj, attr)
    return obj


class JobStatus(str, Enum):
    QUEUED = "queued"
    DEFERRED = "deferred"
    STARTED = "started"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELED = "canceled"
    STOPPED = "stopped"


class Job:
    """Represents a single job in the rustycluster queue system.

    Mirrors the rq.job.Job interface — use Job.fetch() to load an existing
    job, or create one via Queue.enqueue().
    """

    def __init__(self, job_id: str, connection: "RustyClusterClient") -> None:
        self._conn = connection
        self._id = job_id

        # Populated by _load() / set directly during enqueue
        self._status: JobStatus = JobStatus.QUEUED
        self._func_name: str = ""
        self._args_b64: str = ""
        self._kwargs_b64: str = ""
        self._result_b64: str = ""
        self._exc_info: str = ""
        self._origin: str = ""
        self._enqueued_at: Optional[str] = None
        self._started_at: Optional[str] = None
        self._ended_at: Optional[str] = None
        self._retries_left: int = 0
        self._retries_total: int = 0  # original max, used to compute attempt index
        self._retry_intervals: str = "0"  # comma-separated ints
        self._description: str = ""
        self._job_timeout: int = 180
        self._result_ttl: int = 500
        self._failure_ttl: int = -1
        self._meta: dict = {}
        self._depends_on: str = ""  # comma-separated dependency job IDs

    # ------------------------------------------------------------------
    # RQ-compatible public properties
    # ------------------------------------------------------------------

    @property
    def id(self) -> str:
        return self._id

    @property
    def origin(self) -> str:
        return self._origin

    @property
    def enqueued_at(self) -> Optional[datetime]:
        return _parse_dt(self._enqueued_at)

    @property
    def started_at(self) -> Optional[datetime]:
        return _parse_dt(self._started_at)

    @property
    def ended_at(self) -> Optional[datetime]:
        return _parse_dt(self._ended_at)

    @property
    def result(self) -> Any:
        return _deserialize(self._result_b64)

    @property
    def exc_info(self) -> str:
        return self._exc_info

    @property
    def description(self) -> str:
        return self._description

    @property
    def timeout(self) -> int:
        return self._job_timeout

    @property
    def meta(self) -> dict:
        return self._meta

    @meta.setter
    def meta(self, value: dict) -> None:
        self._meta = value

    # ------------------------------------------------------------------
    # RQ-compatible methods
    # ------------------------------------------------------------------

    def get_status(self) -> JobStatus:
        data = self._conn.hget_all(_JOB_KEY_PREFIX + self._id)
        if not data:
            raise ValueError(f"Job {self._id} not found")
        return JobStatus(data.get("status", JobStatus.QUEUED))

    def cancel(self) -> None:
        """Cancel a queued job. Has no effect on already-executing jobs."""
        self._status = JobStatus.CANCELED
        self._conn.hset(_JOB_KEY_PREFIX + self._id, "status", JobStatus.CANCELED.value)
        # Best-effort removal from the queue list — lrem count=0 removes all occurrences
        if self._origin:
            self._conn.lrem(f"rc:queue:{self._origin}", 0, self._id)

    def refresh(self) -> None:
        """Reload all fields from the cluster."""
        self._load(self._conn.hget_all(_JOB_KEY_PREFIX + self._id))

    def delete(self) -> None:
        """Delete this job's data from the cluster."""
        self._delete()

    def save_meta(self) -> None:
        """Persist job.meta to the cluster."""
        self._conn.hset(self._hash_key(), "meta_b64", _serialize(self._meta))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _hash_key(self) -> str:
        return _JOB_KEY_PREFIX + self._id

    def _to_dict(self) -> dict[str, str]:
        return {
            "id": self._id,
            "status": self._status.value,
            "func_name": self._func_name,
            "args_b64": self._args_b64,
            "kwargs_b64": self._kwargs_b64,
            "result_b64": self._result_b64,
            "exc_info": self._exc_info,
            "origin": self._origin,
            "enqueued_at": self._enqueued_at or "",
            "started_at": self._started_at or "",
            "ended_at": self._ended_at or "",
            "retries_left": str(self._retries_left),
            "retries_total": str(self._retries_total),
            "retry_intervals": self._retry_intervals,
            "description": self._description,
            "job_timeout": str(self._job_timeout),
            "result_ttl": str(self._result_ttl),
            "failure_ttl": str(self._failure_ttl),
            "meta_b64": _serialize(self._meta),
            "depends_on": self._depends_on,
        }

    def _save(self) -> None:
        self._conn.hmset(self._hash_key(), self._to_dict())

    def _delete(self) -> None:
        self._conn.delete(self._hash_key())

    def _load(self, data: dict[str, str]) -> None:
        self._status = JobStatus(data.get("status", JobStatus.QUEUED.value))
        self._func_name = data.get("func_name", "")
        self._args_b64 = data.get("args_b64", "")
        self._kwargs_b64 = data.get("kwargs_b64", "")
        self._result_b64 = data.get("result_b64", "")
        self._exc_info = data.get("exc_info", "")
        self._origin = data.get("origin", "")
        self._enqueued_at = data.get("enqueued_at") or None
        self._started_at = data.get("started_at") or None
        self._ended_at = data.get("ended_at") or None
        self._retries_left = int(data.get("retries_left", "0"))
        self._retries_total = int(data.get("retries_total", "0"))
        self._retry_intervals = data.get("retry_intervals", "0")
        self._description = data.get("description", "")
        self._job_timeout = int(data.get("job_timeout", "180"))
        self._result_ttl = int(data.get("result_ttl", "500"))
        self._failure_ttl = int(data.get("failure_ttl", "-1"))
        self._meta = _deserialize(data.get("meta_b64")) or {}
        self._depends_on = data.get("depends_on", "")

    def _get_callable(self) -> Any:
        return _ref_to_func(self._func_name)

    def _get_args(self) -> tuple:
        return _deserialize(self._args_b64) or ()

    def _get_kwargs(self) -> dict:
        return _deserialize(self._kwargs_b64) or {}

    # ------------------------------------------------------------------
    # Factory / class methods
    # ------------------------------------------------------------------

    @classmethod
    def fetch(cls, job_id: str, connection: "RustyClusterClient") -> "Job":
        """Load a job from the cluster by ID. Mirrors rq.job.Job.fetch()."""
        job = cls(job_id, connection)
        data = connection.hget_all(_JOB_KEY_PREFIX + job_id)
        if not data:
            raise ValueError(f"Job {job_id} not found in cluster")
        job._load(data)
        return job

    @classmethod
    def _create(
        cls,
        connection: "RustyClusterClient",
        fn: Any,
        args: tuple,
        kwargs: dict,
        origin: str,
        job_id: Optional[str] = None,
        description: str = "",
        job_timeout: int = 180,
        result_ttl: int = 500,
        failure_ttl: int = -1,
        retry_max: int = 0,
        retry_intervals: list[int] | None = None,
    ) -> "Job":
        job = cls(job_id or str(uuid.uuid4()), connection)
        job._func_name = _func_to_ref(fn)
        job._args_b64 = _serialize(args)
        job._kwargs_b64 = _serialize(kwargs)
        job._origin = origin
        job._enqueued_at = _now_iso()
        job._status = JobStatus.QUEUED
        job._description = description or f"{fn.__qualname__}"
        job._job_timeout = job_timeout
        job._result_ttl = result_ttl
        job._failure_ttl = failure_ttl
        job._retries_left = retry_max
        job._retries_total = retry_max
        job._retry_intervals = ",".join(str(i) for i in (retry_intervals or [0]))
        job._save()
        return job
