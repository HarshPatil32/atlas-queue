from typing import cast

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.models import Job, JobStatus

_CLAIMABLE_STATUSES = (
    JobStatus.QUEUED.value,
    JobStatus.SCHEDULED.value,
    JobStatus.RETRYING.value,
)


async def claim_jobs(
    session: AsyncSession,
    *,
    queues: list[str],
    worker_name: str,
    limit: int,
    lease_seconds: int,
) -> list[Job]:
    if limit <= 0 or not queues:
        return []
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be > 0")

    select_stmt = (
        select(Job.id)
        .where(
            Job.queue.in_(queues),
            Job.status.in_(_CLAIMABLE_STATUSES),
            Job.next_run_at <= func.now(),
        )
        .order_by(
            Job.priority.desc(),
            Job.next_run_at.asc(),
            Job.created_at.asc(),
        )
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(select_stmt)
    job_ids = list(result.scalars().all())
    if not job_ids:
        return []

    claim_time = func.now()
    lease_duration = text("make_interval(secs => :lease_seconds)").bindparams(
        lease_seconds=lease_seconds
    )
    update_stmt = (
        update(Job)
        .where(Job.id.in_(job_ids))
        .values(
            status=JobStatus.RUNNING.value,
            locked_by=worker_name,
            locked_at=claim_time,
            lease_expires_at=claim_time + lease_duration,
            attempts=Job.attempts + 1,
            updated_at=claim_time,
        )
        .returning(Job)
    )
    update_result = await session.execute(update_stmt)
    claimed_by_id = {
        job.id: job for job in cast(list[Job], update_result.scalars().all())
    }
    await session.commit()
    return [claimed_by_id[job_id] for job_id in job_ids if job_id in claimed_by_id]
