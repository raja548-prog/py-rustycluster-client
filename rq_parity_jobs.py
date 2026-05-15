"""
rq_parity_jobs.py

Module-level callables used by test_rq_parity.py.

Must live in a proper importable module (not __main__) so that both
python-rq workers (which reject __main__ functions) and rustycluster.rq
workers can resolve them via importlib.
"""

from __future__ import annotations

import time


def add(x, y):
    return x + y


def add_kwargs(x, *, offset=0):
    return x + offset


def raises():
    raise ValueError("intentional failure")


def slow(seconds):
    time.sleep(seconds)
    return "done"


def return_current_job_id():
    """Return the executing job's ID using whichever get_current_job() is in scope."""
    job = None
    try:
        from rq import get_current_job
        job = get_current_job()
    except Exception:
        pass
    if job is None:
        try:
            from rustycluster.rq.worker import get_current_job
            job = get_current_job()
        except Exception:
            pass
    return job.id if job else "NO_JOB"
