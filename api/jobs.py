from typing import Annotated

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from api.deps import get_db
from core.models import Job
from core.schemas import JobCreateRequest, JobResponse

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
