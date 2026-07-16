import asyncio
from datetime import UTC, datetime

from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.models import Job, JobStatus


def _job(
    *,
    job_id: int | None = None,
    queue: str,
    job_type: str,
    idempotency_key: str,
    created_at: datetime,
) -> Job:
    return Job(
        id=job_id,
        queue=queue,
        job_type=job_type,
        payload_json={},
        status=JobStatus.QUEUED.value,
        priority=0,
        attempts=0,
        max_retries=3,
        next_run_at=created_at,
        timeout_seconds=60,
        idempotency_key=idempotency_key,
        created_at=created_at,
        updated_at=created_at,
    )


async def _seed_job_with_idempotency_key(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    idempotency_key: str,
    queue: str = "emails",
) -> Job:
    created_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    job = _job(
        queue=queue,
        job_type="send_email",
        idempotency_key=idempotency_key,
        created_at=created_at,
    )
    async with sessionmaker() as session:
        session.add(job)
        await session.commit()
        await session.refresh(job)
    return job


async def test_create_job_allows_same_idempotency_key_in_different_queue(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    await _seed_job_with_idempotency_key(
        sessionmaker,
        idempotency_key="order-789",
        queue="emails",
    )

    response = await http_client.post(
        "/jobs",
        json={
            "queue": "sms",
            "job_type": "send_sms",
            "idempotency_key": "order-789",
        },
    )

    assert response.status_code == 201

    async with sessionmaker() as session:
        count = (
            await session.execute(
                select(func.count())
                .select_from(Job)
                .where(Job.idempotency_key == "order-789")
            )
        ).scalar_one()
        assert count == 2


async def test_create_job_returns_409_when_idempotency_key_conflicts(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    await _seed_job_with_idempotency_key(sessionmaker, idempotency_key="order-123")

    response = await http_client.post(
        "/jobs",
        json={
            "queue": "emails",
            "job_type": "send_email",
            "idempotency_key": "order-123",
        },
    )

    assert response.status_code == 409
    assert response.json() == {
        "detail": "Job with this idempotency_key already exists in this queue"
    }

    async with sessionmaker() as session:
        count = (
            await session.execute(
                select(func.count())
                .select_from(Job)
                .where(Job.idempotency_key == "order-123")
            )
        ).scalar_one()
        assert count == 1


async def test_create_job_is_atomic_under_concurrent_idempotency_requests(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    payload = {
        "queue": "emails",
        "job_type": "send_email",
        "idempotency_key": "order-456",
    }

    response_one, response_two = await asyncio.gather(
        http_client.post("/jobs", json=payload),
        http_client.post("/jobs", json=payload),
    )

    status_codes = sorted([response_one.status_code, response_two.status_code])
    assert status_codes == [201, 409]

    async with sessionmaker() as session:
        count = (
            await session.execute(
                select(func.count())
                .select_from(Job)
                .where(Job.idempotency_key == "order-456")
            )
        ).scalar_one()
        assert count == 1
