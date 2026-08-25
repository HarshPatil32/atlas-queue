from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from api.deps import get_db
from core.models import Job, JobStatus
from core.schemas import DEFAULT_LIMIT, MAX_LIMIT, JobListResponse, JobResponse

router = APIRouter(prefix="/dead-letter", tags=["dead-letter"])


@router.get("", response_model=JobListResponse)
async def list_dead_letter_jobs(
    session: Annotated[AsyncSession, Depends(get_db)],
    queue: Annotated[str | None, Query(min_length=1, max_length=255)] = None,
    job_type: Annotated[str | None, Query(min_length=1, max_length=255)] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> JobListResponse:
    filters = [Job.status == JobStatus.DEAD_LETTER.value]
    if queue is not None:
        filters.append(Job.queue == queue)
    if job_type is not None:
        filters.append(Job.job_type == job_type)

    total = (
        await session.execute(select(func.count()).select_from(Job).where(*filters))
    ).scalar_one()

    rows = (
        (
            await session.execute(
                select(Job)
                .where(*filters)
                .order_by(Job.dead_lettered_at.desc().nulls_last(), Job.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )
        .scalars()
        .all()
    )

    return JobListResponse(jobs=list(rows), total=total, limit=limit, offset=offset)


@router.post("/{job_id}/retry", response_model=JobResponse)
async def retry_dead_letter_job(
    job_id: int,
    session: Annotated[AsyncSession, Depends(get_db)],
) -> Job:
    result = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.DEAD_LETTER.value)
        .values(
            status=JobStatus.QUEUED.value,
            attempts=0,
            next_run_at=func.now(),
            last_error=None,
            failed_at=None,
            dead_lettered_at=None,
            locked_by=None,
            locked_at=None,
            lease_expires_at=None,
            updated_at=func.now(),
        )
        .returning(Job)
    )
    retried_job = result.scalars().one_or_none()
    if retried_job is not None:
        await session.commit()
        return retried_job

    existing_job = await session.get(Job, job_id)
    if existing_job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    raise HTTPException(
        status_code=409,
        detail="Job is not in dead_letter status",
    )
