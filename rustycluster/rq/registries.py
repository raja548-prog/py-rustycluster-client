"""Job registries — RQ-compatible interface over rustycluster sorted sets.

Each registry stores job IDs as members of a sorted set scored by the
Unix timestamp of when the job entered that state. This matches RQ's
internal design and gives chronological ordering for free.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from rustycluster.client import RustyClusterClient
    from rustycluster.rq.queue import Queue


class _BaseRegistry:
    key_prefix: str  # subclass must set this

    def __init__(
        self,
        queue: Optional["Queue"] = None,
        name: Optional[str] = None,
        connection: Optional["RustyClusterClient"] = None,
    ) -> None:
        if queue is not None:
            self._name = queue.name
            self._conn = queue._conn
        elif name is not None and connection is not None:
            self._name = name
            self._conn = connection
        else:
            raise ValueError("Pass either queue= or both name= and connection=")

    @property
    def key(self) -> str:
        return f"{self.key_prefix}:{self._name}"

    def get_job_ids(self, start: int = 0, end: int = -1) -> list[str]:
        """Return job IDs ordered by entry time (ascending)."""
        # zrange_by_score with full range; use a large upper bound for "+inf"
        members = self._conn.zrange_by_score(self.key, 0, float("inf"))
        if end == -1:
            return members[start:]
        return members[start : end + 1]

    def __len__(self) -> int:
        return self._conn.zcard(self.key)

    # ------------------------------------------------------------------
    # Internal helpers used by Worker
    # ------------------------------------------------------------------

    def add(self, job_id: str, score: Optional[float] = None) -> None:
        ts = score if score is not None else time.time()
        self._conn.zadd(self.key, ts, job_id)

    def remove(self, job_id: str) -> None:
        self._conn.zrem(self.key, job_id)

    def contains(self, job_id: str) -> bool:
        return job_id in self.get_job_ids()


class StartedJobRegistry(_BaseRegistry):
    """Tracks jobs that are currently being executed by a worker."""

    key_prefix = "rc:started"


class FinishedJobRegistry(_BaseRegistry):
    """Tracks jobs that completed successfully."""

    key_prefix = "rc:finished"


class FailedJobRegistry(_BaseRegistry):
    """Tracks jobs that failed after exhausting all retries."""

    key_prefix = "rc:failed"


class DeferredJobRegistry(_BaseRegistry):
    """Tracks jobs waiting for a dependency to finish."""

    key_prefix = "rc:deferred"
