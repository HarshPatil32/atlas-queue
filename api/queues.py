from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.deps import get_db
from core.models import Job
from core.schemas import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    QueueListResponse,
    QueueName,
    QueueResponse,
)

router = APIRouter(prefix="/queues", tags=["queues"])


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
