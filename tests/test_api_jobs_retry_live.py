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
    attempts: int = 0,
    last_error: str | None = None,
    failed_at: datetime | None = None,
    dead_lettered_at: datetime | None = None,
    locked_by: str | None = None,
    locked_at: datetime | None = None,
    lease_expires_at: datetime | None = None,
) -> Job:
    return Job(
        id=job_id,
        queue=queue,
        job_type=job_type,
        payload_json={},
        status=job_status,
        priority=0,
        attempts=attempts,
        max_retries=3,
        next_run_at=created_at,
        timeout_seconds=60,
        last_error=last_error,
        failed_at=failed_at,
        dead_lettered_at=dead_lettered_at,
        locked_by=locked_by,
        locked_at=locked_at,
        lease_expires_at=lease_expires_at,
        created_at=created_at,
        updated_at=created_at,
    )


async def _seed_job(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    job_status: str,
    attempts: int = 0,
    last_error: str | None = None,
    failed_at: datetime | None = None,
    dead_lettered_at: datetime | None = None,
) -> Job:
    created_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    job = _job(
        queue="emails",
        job_type="send_email",
        job_status=job_status,
        created_at=created_at,
        attempts=attempts,
        last_error=last_error,
        failed_at=failed_at,
        dead_lettered_at=dead_lettered_at,
    )
    async with sessionmaker() as session:
        session.add(job)
        await session.commit()
        await session.refresh(job)
    return job


@pytest.mark.parametrize(
    "job_status",
    [JobStatus.FAILED.value, JobStatus.DEAD_LETTER.value],
)
async def test_retry_job_persists_queued_status_and_resets_fields(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    job_status: str,
) -> None:
    http_client, sessionmaker = live_client
    job = await _seed_job(
        sessionmaker,
        job_status=job_status,
        attempts=3,
        last_error="boom",
        failed_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
        dead_lettered_at=(
            datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
            if job_status == JobStatus.DEAD_LETTER.value
            else None
        ),
    )

    response = await http_client.post(f"/jobs/{job.id}/retry")

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == job.id
    assert body["status"] == "queued"
    assert body["attempts"] == 0
    assert body["last_error"] is None
    assert body["failed_at"] is None
    assert body["dead_lettered_at"] is None

    async with sessionmaker() as session:
        persisted = await session.get(Job, job.id)
        assert persisted is not None
        assert persisted.status == JobStatus.QUEUED.value
        assert persisted.attempts == 0
        assert persisted.last_error is None
        assert persisted.failed_at is None
        assert persisted.dead_lettered_at is None
        assert persisted.locked_by is None
        assert persisted.locked_at is None
        assert persisted.lease_expires_at is None
        assert persisted.updated_at > job.updated_at
        assert persisted.next_run_at > job.next_run_at


async def test_retry_job_returns_409_and_leaves_running_job_unchanged(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    job = await _seed_job(sessionmaker, job_status=JobStatus.RUNNING.value)

    response = await http_client.post(f"/jobs/{job.id}/retry")

    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job cannot be retried from status '{JobStatus.RUNNING.value}'"
    }

    async with sessionmaker() as session:
        persisted = await session.get(Job, job.id)
        assert persisted is not None
        assert persisted.status == JobStatus.RUNNING.value
        assert persisted.attempts == job.attempts
        assert persisted.updated_at == job.updated_at


@pytest.mark.parametrize(
    "job_status",
    [
        JobStatus.QUEUED.value,
        JobStatus.SCHEDULED.value,
        JobStatus.RUNNING.value,
        JobStatus.SUCCEEDED.value,
        JobStatus.RETRYING.value,
        JobStatus.CANCELLED.value,
    ],
)
async def test_retry_job_returns_409_for_non_retryable_statuses(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    job_status: str,
) -> None:
    http_client, sessionmaker = live_client
    job = await _seed_job(sessionmaker, job_status=job_status)

    response = await http_client.post(f"/jobs/{job.id}/retry")

    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job cannot be retried from status '{job_status}'"
    }


async def test_retry_job_returns_404_for_missing_job(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.post("/jobs/999999/retry")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}


async def test_retry_job_is_atomic_under_concurrent_requests(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    job = await _seed_job(
        sessionmaker,
        job_status=JobStatus.FAILED.value,
        attempts=3,
        last_error="boom",
        failed_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
    )

    response_one, response_two = await asyncio.gather(
        http_client.post(f"/jobs/{job.id}/retry"),
        http_client.post(f"/jobs/{job.id}/retry"),
    )

    status_codes = sorted([response_one.status_code, response_two.status_code])
    assert status_codes == [200, 409]

    async with sessionmaker() as session:
        persisted = (
            await session.execute(select(Job).where(Job.id == job.id))
        ).scalar_one()
        assert persisted.status == JobStatus.QUEUED.value
        assert persisted.attempts == 0
