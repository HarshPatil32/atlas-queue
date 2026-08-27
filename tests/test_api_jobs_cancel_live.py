import asyncio
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.models import Job, JobStatus


def _job(
    *,
    job_id: int | None = None,
    queue: str,
    job_type: str,
    job_status: str,
    created_at: datetime,
    dead_lettered_at: datetime | None = None,
) -> Job:
    return Job(
        id=job_id,
        queue=queue,
        job_type=job_type,
        payload_json={},
        status=job_status,
        priority=0,
        attempts=0,
        max_retries=3,
        next_run_at=created_at,
        timeout_seconds=60,
        dead_lettered_at=dead_lettered_at,
        created_at=created_at,
        updated_at=created_at,
    )


async def _seed_job(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    job_status: str,
    dead_lettered_at: datetime | None = None,
) -> Job:
    created_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    job = _job(
        queue="emails",
        job_type="send_email",
        job_status=job_status,
        created_at=created_at,
        dead_lettered_at=dead_lettered_at,
    )
    async with sessionmaker() as session:
        session.add(job)
        await session.commit()
        await session.refresh(job)
    return job


@pytest.mark.parametrize(
    "job_status",
    [JobStatus.QUEUED.value, JobStatus.SCHEDULED.value, JobStatus.DEAD_LETTER.value],
)
async def test_cancel_job_persists_cancelled_status_for_cancellable_jobs(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    job_status: str,
) -> None:
    http_client, sessionmaker = live_client
    dead_lettered_at = (
        datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        if job_status == JobStatus.DEAD_LETTER.value
        else None
    )
    job = await _seed_job(
        sessionmaker,
        job_status=job_status,
        dead_lettered_at=dead_lettered_at,
    )

    response = await http_client.post(f"/jobs/{job.id}/cancel")

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == job.id
    assert body["status"] == "cancelled"
    assert body["dead_lettered_at"] is None

    async with sessionmaker() as session:
        persisted = await session.get(Job, job.id)
        assert persisted is not None
        assert persisted.status == JobStatus.CANCELLED.value
        assert persisted.dead_lettered_at is None
        assert persisted.updated_at > job.updated_at


async def test_cancel_job_returns_409_and_leaves_running_job_unchanged(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    job = await _seed_job(sessionmaker, job_status=JobStatus.RUNNING.value)

    response = await http_client.post(f"/jobs/{job.id}/cancel")

    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job cannot be cancelled from status '{JobStatus.RUNNING.value}'"
    }

    async with sessionmaker() as session:
        persisted = await session.get(Job, job.id)
        assert persisted is not None
        assert persisted.status == JobStatus.RUNNING.value
        assert persisted.updated_at == job.updated_at


@pytest.mark.parametrize(
    "job_status",
    [
        JobStatus.SUCCEEDED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
    ],
)
async def test_cancel_job_returns_409_for_terminal_statuses(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    job_status: str,
) -> None:
    http_client, sessionmaker = live_client
    job = await _seed_job(sessionmaker, job_status=job_status)

    response = await http_client.post(f"/jobs/{job.id}/cancel")

    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job cannot be cancelled from status '{job_status}'"
    }


async def test_cancel_job_returns_404_for_missing_job(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.post("/jobs/999999/cancel")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}


async def test_cancel_job_is_atomic_under_concurrent_requests(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    job = await _seed_job(sessionmaker, job_status=JobStatus.QUEUED.value)

    response_one, response_two = await asyncio.gather(
        http_client.post(f"/jobs/{job.id}/cancel"),
        http_client.post(f"/jobs/{job.id}/cancel"),
    )

    status_codes = sorted([response_one.status_code, response_two.status_code])
    assert status_codes == [200, 409]

    async with sessionmaker() as session:
        persisted = (
            await session.execute(select(Job).where(Job.id == job.id))
        ).scalar_one()
        assert persisted.status == JobStatus.CANCELLED.value
