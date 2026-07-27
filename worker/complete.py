from typing import Any

from sqlalchemy import func, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.models import Job, JobAttempt, JobAttemptStatus, JobStatus
from worker.execute import JobExecutionResult

_MAX_RETRY_DELAY_SECONDS: int = 300


def _retry_delay_seconds(attempts: int) -> int:
    exponential_delay = 1 << attempts
    if exponential_delay > _MAX_RETRY_DELAY_SECONDS:
        return _MAX_RETRY_DELAY_SECONDS
    return exponential_delay


async def mark_job_succeeded(
    session: AsyncSession,
    *,
    outcome: JobExecutionResult,
    worker_name: str,
) -> None:
    if not outcome.succeeded:
        raise ValueError("mark_job_succeeded requires a succeeded outcome")

    job = outcome.job
    now = func.now()
    updated_job_id = await session.scalar(
        update(Job)
        .where(Job.id == job.id, Job.locked_by == worker_name)
        .values(
            status=JobStatus.SUCCEEDED.value,
            completed_at=now,
            locked_by=None,
            locked_at=None,
            lease_expires_at=None,
            updated_at=now,
        )
        .returning(Job.id)
    )
    if updated_job_id is not None:
        # started_at defaults to server now() at completion; no claim-time row yet.
        session.add(
            JobAttempt(
                job_id=job.id,
                worker_id=worker_name,
                attempt_number=job.attempts,
                status=JobAttemptStatus.SUCCEEDED.value,
                finished_at=now,
                runtime_ms=outcome.runtime_ms,
            )
        )
    await session.commit()


async def mark_job_failed(
    session: AsyncSession,
    *,
    outcome: JobExecutionResult,
    worker_name: str,
) -> None:
    if outcome.succeeded:
        raise ValueError("mark_job_failed requires a failed outcome")

    job = outcome.job
    will_retry = job.attempts < job.max_retries
    next_status = JobStatus.RETRYING.value if will_retry else JobStatus.FAILED.value
    error_text = str(outcome.error) if outcome.error is not None else None
    now = func.now()

    values: dict[str, Any] = {
        "status": next_status,
        "last_error": error_text,
        "locked_by": None,
        "locked_at": None,
        "lease_expires_at": None,
        "updated_at": now,
    }
    if will_retry:
        retry_interval = text("make_interval(secs => :retry_delay_seconds)").bindparams(
            retry_delay_seconds=_retry_delay_seconds(job.attempts),
        )
        values["next_run_at"] = now + retry_interval
    else:
        values["failed_at"] = now

    updated_job_id = await session.scalar(
        update(Job)
        .where(Job.id == job.id, Job.locked_by == worker_name)
        .values(**values)
        .returning(Job.id)
    )
    if updated_job_id is not None:
        session.add(
            JobAttempt(
                job_id=job.id,
                worker_id=worker_name,
                attempt_number=job.attempts,
                status=JobAttemptStatus.FAILED.value,
                finished_at=now,
                error_message=error_text,
                runtime_ms=outcome.runtime_ms,
            )
        )
    await session.commit()
