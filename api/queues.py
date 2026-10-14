from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.deps import get_db
from core.models import Job, JobStatus
from core.schemas import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    QueueListResponse,
    QueueName,
    QueueOldestJobResponse,
    QueueResponse,
    QueueStatsResponse,
)

router = APIRouter(prefix="/queues", tags=["queues"])

_DEPTH_STATUSES = (JobStatus.QUEUED, JobStatus.SCHEDULED, JobStatus.RETRYING)


@router.get("", response_model=QueueListResponse)
async def list_queues(
    session: Annotated[AsyncSession, Depends(get_db)],
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> QueueListResponse:
    total = (
        await session.execute(select(func.count(distinct(Job.queue))))
    ).scalar_one()

    rows = (
        await session.execute(
            select(Job.queue, func.count().label("job_count"))
            .group_by(Job.queue)
            .order_by(Job.queue)
            .limit(limit)
            .offset(offset)
        )
    ).all()

    return QueueListResponse(
        queues=[QueueResponse.model_validate(row) for row in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{queue_name}", response_model=QueueResponse)
async def get_queue(
    queue_name: Annotated[QueueName, Path()],
    session: Annotated[AsyncSession, Depends(get_db)],
) -> QueueResponse:
    row = (
        await session.execute(
            select(Job.queue, func.count().label("job_count"))
            .where(Job.queue == queue_name)
            .group_by(Job.queue)
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Queue not found")
    return QueueResponse.model_validate(row)


@router.get("/{queue_name}/stats", response_model=QueueStatsResponse)
async def get_queue_stats(
    queue_name: Annotated[QueueName, Path()],
    session: Annotated[AsyncSession, Depends(get_db)],
) -> QueueStatsResponse:
    count_rows = (
        await session.execute(
            select(Job.status, func.count())
            .where(Job.queue == queue_name)
            .group_by(Job.status)
        )
    ).all()

    if not count_rows:
        raise HTTPException(status_code=404, detail="Queue not found")

    status_counts = dict.fromkeys(JobStatus, 0)
    for status, count in count_rows:
        status_counts[JobStatus(status)] = count

    depth = sum(status_counts[status] for status in _DEPTH_STATUSES)

    oldest_job = None
    if depth > 0:
        row = (
            await session.execute(
                select(
                    Job.id,
                    Job.job_type,
                    Job.status,
                    Job.created_at,
                    Job.next_run_at,
                )
                .where(
                    Job.queue == queue_name,
                    Job.status.in_([s.value for s in _DEPTH_STATUSES]),
                )
                .order_by(Job.created_at, Job.id)
                .limit(1)
            )
        ).one_or_none()
        if row is not None:
            oldest_job = QueueOldestJobResponse.model_validate(row)

    return QueueStatsResponse(
        queue=queue_name,
        depth=depth,
        status_counts=status_counts,
        oldest_job=oldest_job,
    )
