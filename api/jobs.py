from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from api.deps import get_db
from core.models import Job, JobStatus
from core.schemas import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    JobCreateRequest,
    JobListResponse,
    JobResponse,
)

router = APIRouter(prefix="/jobs", tags=["jobs"])

_CANCELLABLE_STATUSES = (JobStatus.QUEUED.value, JobStatus.SCHEDULED.value)
_RETRYABLE_STATUSES = (JobStatus.FAILED.value, JobStatus.DEAD_LETTER.value)


@router.post("", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_job(
    payload: JobCreateRequest,
    session: Annotated[AsyncSession, Depends(get_db)],
) -> Job:
    job = Job(
        queue=payload.queue,
        job_type=payload.job_type,
        payload_json=payload.payload,
        priority=payload.priority,
        max_retries=payload.max_retries,
        timeout_seconds=payload.timeout_seconds,
        idempotency_key=payload.idempotency_key,
    )
    if payload.run_at is not None:
        job.next_run_at = payload.run_at
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


@router.get("", response_model=JobListResponse)
async def list_jobs(
    session: Annotated[AsyncSession, Depends(get_db)],
    queue: Annotated[str | None, Query(min_length=1, max_length=255)] = None,
    job_status: Annotated[JobStatus | None, Query(alias="status")] = None,
    job_type: Annotated[str | None, Query(min_length=1, max_length=255)] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> JobListResponse:
    filters = []
    if queue is not None:
        filters.append(Job.queue == queue)
    if job_status is not None:
        filters.append(Job.status == job_status.value)
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
                .order_by(Job.created_at.desc(), Job.id.desc())
                .limit(limit)
                .offset(offset)
            )
        )
        .scalars()
        .all()
    )

    return JobListResponse(jobs=list(rows), total=total, limit=limit, offset=offset)


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: int,
    session: Annotated[AsyncSession, Depends(get_db)],
) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post("/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(
    job_id: int,
    session: Annotated[AsyncSession, Depends(get_db)],
) -> Job:
    result = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status.in_(_CANCELLABLE_STATUSES))
        .values(status=JobStatus.CANCELLED.value, updated_at=func.now())
        .returning(Job)
    )
    cancelled_job = result.scalars().one_or_none()
    if cancelled_job is not None:
        await session.commit()
        return cancelled_job

    existing_job = await session.get(Job, job_id)
    if existing_job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    raise HTTPException(
        status_code=409,
        detail=f"Job cannot be cancelled from status '{existing_job.status}'",
    )


@router.post("/{job_id}/retry", response_model=JobResponse)
async def retry_job(
    job_id: int,
    session: Annotated[AsyncSession, Depends(get_db)],
) -> Job:
    result = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status.in_(_RETRYABLE_STATUSES))
        .values(
            status=JobStatus.QUEUED.value,
            # Fresh run budget; worker logic should compare attempts to max_retries.
            attempts=0,
            # Immediate retry only; no optional run_at delay on this endpoint.
            next_run_at=func.now(),
            last_error=None,
            failed_at=None,
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
        detail=f"Job cannot be retried from status '{existing_job.status}'",
    )
