from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.models import Job, JobStatus
from core.schemas import DEFAULT_LIMIT


def _job(
    *,
    queue: str,
    job_type: str,
    job_status: str,
    created_at: datetime,
) -> Job:
    return Job(
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
async def seeded_jobs(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> list[Job]:
    _http_client, sessionmaker = live_client
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    jobs = [
        _job(
            queue="emails",
            job_type="send_email",
            job_status=JobStatus.QUEUED.value,
            created_at=base_time,
        ),
        _job(
            queue="emails",
            job_type="send_email",
            job_status=JobStatus.RUNNING.value,
            created_at=base_time + timedelta(minutes=1),
        ),
        _job(
            queue="emails",
            job_type="process_image",
            job_status=JobStatus.QUEUED.value,
            created_at=base_time + timedelta(minutes=2),
        ),
        _job(
            queue="reports",
            job_type="generate_report",
            job_status=JobStatus.SUCCEEDED.value,
            created_at=base_time + timedelta(minutes=3),
        ),
        _job(
            queue="reports",
            job_type="generate_report",
            job_status=JobStatus.FAILED.value,
            created_at=base_time + timedelta(minutes=4),
        ),
    ]

    async with sessionmaker() as session:
        session.add_all(jobs)
        await session.commit()
        for job in jobs:
            await session.refresh(job)

    return jobs


async def test_list_queues_returns_counts_ordered_by_name(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/queues")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["limit"] == DEFAULT_LIMIT
    assert body["offset"] == 0
    assert body["queues"] == [
        {"queue": "emails", "job_count": 3},
        {"queue": "reports", "job_count": 2},
    ]


async def test_list_queues_pagination(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/queues?limit=1&offset=1")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["limit"] == 1
    assert body["offset"] == 1
    assert body["queues"] == [{"queue": "reports", "job_count": 2}]


async def test_list_queues_offset_beyond_total_returns_empty_page(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/queues?offset=10")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["offset"] == 10
    assert body["queues"] == []


async def test_get_queue_returns_count(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/queues/emails")

    assert response.status_code == 200
    assert response.json() == {"queue": "emails", "job_count": 3}


async def test_get_queue_not_found(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/queues/unknown")

    assert response.status_code == 404
    assert response.json() == {"detail": "Queue not found"}


async def test_list_queues_empty_database(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/queues")

    assert response.status_code == 200
    body = response.json()
    assert body["queues"] == []
    assert body["total"] == 0
