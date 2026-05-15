"""Retry policy — mirrors rq.Retry."""

from __future__ import annotations


class Retry:
    """Retry policy for a job, identical interface to rq.Retry.

    Args:
        max: Maximum number of retries (not counting the initial attempt).
        interval: Seconds to wait before each retry attempt. If shorter than
            ``max``, the last value is repeated. Defaults to [0] (no delay).
    """

    def __init__(self, max: int = 3, interval: list[int] | int | None = None) -> None:
        self.max = max
        if interval is None:
            self.interval = [0]
        elif isinstance(interval, int):
            self.interval = [interval]
        else:
            self.interval = list(interval)

    def get_interval(self, attempt: int) -> int:
        """Return the wait interval (seconds) for the given retry attempt (0-based)."""
        idx = min(attempt, len(self.interval) - 1)
        return self.interval[idx]
