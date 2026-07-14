from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.models import Job, JobStatus


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


async def test_list_jobs_filters_by_queue_status_and_job_type(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get(
        "/jobs?queue=emails&status=queued&job_type=send_email"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert len(body["jobs"]) == 1
    assert body["jobs"][0]["job_id"] == seeded_jobs[0].id
    assert body["jobs"][0]["queue"] == "emails"
    assert body["jobs"][0]["status"] == "queued"
    assert body["jobs"][0]["job_type"] == "send_email"


async def test_list_jobs_returns_empty_when_no_matches(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/jobs?queue=missing")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 0
    assert body["jobs"] == []


async def test_list_jobs_orders_by_created_at_desc_then_id_desc(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/jobs")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 5
    returned_ids = [job["job_id"] for job in body["jobs"]]
    jobs_by_id = {job.id: job for job in seeded_jobs}
    expected_ids = sorted(
        (job.id for job in seeded_jobs),
        key=lambda job_id: jobs_by_id[job_id].created_at,
        reverse=True,
    )
    assert returned_ids == expected_ids


async def test_list_jobs_paginates_with_total_independent_of_limit(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/jobs?limit=2&offset=1")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 5
    assert body["limit"] == 2
    assert body["offset"] == 1
    assert len(body["jobs"]) == 2
