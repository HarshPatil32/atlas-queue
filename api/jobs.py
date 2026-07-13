from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
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
