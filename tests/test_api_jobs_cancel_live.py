import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import pool, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from api.deps import get_db
from api.main import app
from core.db import Base
from core.models import Job, JobStatus
from tests.test_migrations import (
    LIVE_MIGRATIONS_TEST_DATABASE_URL,
    _postgres_is_reachable,
)


def _job(
    *,
    job_id: int | None = None,
    queue: str,
    job_type: str,
    job_status: str,
    created_at: datetime,
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
        created_at=created_at,
        updated_at=created_at,
    )


@pytest.fixture
async def live_client() -> (
    AsyncIterator[tuple[AsyncClient, async_sessionmaker[AsyncSession]]]
):
    live_url = os.environ.get(LIVE_MIGRATIONS_TEST_DATABASE_URL)
    if live_url is None:
        pytest.skip(
            "Set LIVE_MIGRATIONS_TEST_DATABASE_URL to a disposable Postgres "
            "database to run live cancel-job verification."
        )
    assert live_url is not None

    if not await _postgres_is_reachable(live_url):
        pytest.skip("Postgres is not reachable at LIVE_MIGRATIONS_TEST_DATABASE_URL")

    engine = create_async_engine(live_url, poolclass=pool.NullPool)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with sessionmaker() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client, sessionmaker

    app.dependency_overrides.pop(get_db, None)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
    await engine.dispose()


async def _seed_job(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    job_status: str,
) -> Job:
    created_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    job = _job(
        queue="emails",
        job_type="send_email",
        job_status=job_status,
        created_at=created_at,
    )
    async with sessionmaker() as session:
        session.add(job)
        await session.commit()
        await session.refresh(job)
    return job


@pytest.mark.parametrize(
    "job_status",
    [JobStatus.QUEUED.value, JobStatus.SCHEDULED.value],
)
async def test_cancel_job_persists_cancelled_status_for_cancellable_jobs(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    job_status: str,
) -> None:
    http_client, sessionmaker = live_client
    job = await _seed_job(sessionmaker, job_status=job_status)

    response = await http_client.post(f"/jobs/{job.id}/cancel")

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == job.id
    assert body["status"] == "cancelled"

    async with sessionmaker() as session:
        persisted = await session.get(Job, job.id)
        assert persisted is not None
        assert persisted.status == JobStatus.CANCELLED.value
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
        JobStatus.DEAD_LETTER.value,
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
