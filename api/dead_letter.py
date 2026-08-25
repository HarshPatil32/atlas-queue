from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.deps import get_db
from core.models import Job, JobStatus
from core.schemas import DEFAULT_LIMIT, MAX_LIMIT, JobListResponse

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
