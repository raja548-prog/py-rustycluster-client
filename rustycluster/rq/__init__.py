"""rustycluster.rq — RQ-compatible job queue backed by a rustycluster cluster.

Drop-in replacement for Python RQ. Only the connection object changes:

    # Before (RQ):
    from rq import Queue, Worker, Retry
    from rq.job import Job
    from rq.registries import StartedJobRegistry, FinishedJobRegistry, FailedJobRegistry
    from rq import get_current_job
    conn = Redis(host="localhost", port=6379)

    # After (rustycluster.rq):
    from rustycluster.rq import (
        Queue, Worker, Retry, Job, JobStatus,
        StartedJobRegistry, FinishedJobRegistry, FailedJobRegistry,
        get_current_job,
    )
    from rustycluster import get_client
    conn = get_client("DB0")    # <- only this line changes

Everything else — Queue(name, connection=conn), q.enqueue(fn, arg, retry=Retry(max=3)),
Job.fetch(id, connection=conn), worker.work(burst=True), registry.get_job_ids() — is identical.
"""

from rustycluster.rq.job import Job, JobStatus
from rustycluster.rq.queue import Queue
from rustycluster.rq.registries import DeferredJobRegistry, FailedJobRegistry, FinishedJobRegistry, StartedJobRegistry
from rustycluster.rq.retry import Retry
from rustycluster.rq.worker import Worker, get_current_job

__all__ = [
    "Queue",
    "Worker",
    "Job",
    "JobStatus",
    "Retry",
    "StartedJobRegistry",
    "FinishedJobRegistry",
    "FailedJobRegistry",
    "DeferredJobRegistry",
    "get_current_job",
]
