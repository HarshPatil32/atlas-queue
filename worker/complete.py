from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.models import Job, JobAttempt, JobAttemptStatus, JobStatus
from worker.execute import JobExecutionResult


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
