from typing import Any, cast

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.models import Job, JobAttempt, JobAttemptStatus, JobStatus
from worker.complete import retry_delay_seconds

_CLAIMABLE_STATUSES = (
    JobStatus.QUEUED.value,
    JobStatus.SCHEDULED.value,
    JobStatus.RETRYING.value,
)

_REAP_ERROR_MESSAGE = "lease expired: worker did not renew in time"
_DEFAULT_REAP_LIMIT = 100


async def count_in_flight_jobs(
    session: AsyncSession,
    *,
    worker_name: str,
) -> int:
    stmt = (
        select(func.count())
        .select_from(Job)
        .where(
            Job.locked_by == worker_name,
            Job.status == JobStatus.RUNNING.value,
            Job.lease_expires_at > func.now(),
        )
    )
    return (await session.scalar(stmt)) or 0


async def claim_jobs(
    session: AsyncSession,
    *,
    queues: list[str],
    worker_name: str,
    limit: int,
    lease_seconds: int,
) -> list[Job]:
    """Claim due jobs with SKIP LOCKED select and atomic running/lock/lease transition.

    Also increments attempts for each claimed job.
    """
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


async def extend_lease(
    session: AsyncSession,
    *,
    job_id: int,
    worker_name: str,
    lease_seconds: int,
) -> bool:
    """Extend lease_expires_at for a job this worker currently holds."""
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be > 0")

    now = func.now()
    lease_duration = text("make_interval(secs => :lease_seconds)").bindparams(
        lease_seconds=lease_seconds
    )
    updated_id = await session.scalar(
        update(Job)
        .where(
            Job.id == job_id,
            Job.locked_by == worker_name,
            Job.status == JobStatus.RUNNING.value,
            Job.lease_expires_at > now,
        )
        .values(
            lease_expires_at=now + lease_duration,
            updated_at=now,
        )
        .returning(Job.id)
    )
    await session.commit()
    return updated_id is not None


async def reap_expired_jobs(
    session: AsyncSession,
    *,
    limit: int = _DEFAULT_REAP_LIMIT,
) -> int:
    """Requeue/retry RUNNING jobs whose owning worker's lease expired.

    Cluster-wide (not scoped to a worker_name): the owning worker is presumed
    dead. Mirrors mark_job_failed's retry-vs-terminal decision; attempts is
    already incremented at claim time, so it is not incremented again here.
    """
    if limit <= 0:
        return 0

    select_stmt = (
        select(Job.id)
        .where(
            Job.status == JobStatus.RUNNING.value,
            Job.lease_expires_at < func.now(),
        )
        .order_by(Job.lease_expires_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    job_ids = list((await session.execute(select_stmt)).scalars().all())
    if not job_ids:
        return 0

    jobs = (await session.scalars(select(Job).where(Job.id.in_(job_ids)))).all()

    now = func.now()
    reaped_count = 0
    for job in jobs:
        worker_id = job.locked_by or "unknown"
        will_retry = job.attempts < job.max_retries
        values: dict[str, Any] = {
            "status": (
                JobStatus.RETRYING.value if will_retry else JobStatus.FAILED.value
            ),
            "last_error": _REAP_ERROR_MESSAGE,
            "locked_by": None,
            "locked_at": None,
            "lease_expires_at": None,
            "updated_at": now,
        }
        if will_retry:
            retry_interval = text(
                "make_interval(secs => :retry_delay_seconds)"
            ).bindparams(retry_delay_seconds=retry_delay_seconds(job.attempts))
            values["next_run_at"] = now + retry_interval
        else:
            values["failed_at"] = now

        updated_id = await session.scalar(
            update(Job)
            .where(
                Job.id == job.id,
                Job.status == JobStatus.RUNNING.value,
                Job.lease_expires_at < func.now(),
            )
            .values(**values)
            .returning(Job.id)
        )
        if updated_id is not None:
            session.add(
                JobAttempt(
                    job_id=job.id,
                    worker_id=worker_id,
                    attempt_number=job.attempts,
                    status=JobAttemptStatus.FAILED.value,
                    finished_at=now,
                    error_message=_REAP_ERROR_MESSAGE,
                )
            )
            reaped_count += 1

    await session.commit()
    return reaped_count


async def release_in_flight_jobs(
    session: AsyncSession,
    *,
    worker_name: str,
) -> int:
    """Return this worker's running jobs to queued (graceful shutdown).

    Does not decrement attempts (incremented at claim) or change next_run_at;
    retry vs shutdown semantics for those fields belong in Epic 7.
    """
    stmt = (
        update(Job)
        .where(
            Job.locked_by == worker_name,
            Job.status == JobStatus.RUNNING.value,
        )
        .values(
            status=JobStatus.QUEUED.value,
            locked_by=None,
            locked_at=None,
            lease_expires_at=None,
            updated_at=func.now(),
        )
        .returning(Job.id)
    )
    result = await session.execute(stmt)
    released_count = len(result.scalars().all())
    await session.commit()
    return released_count
