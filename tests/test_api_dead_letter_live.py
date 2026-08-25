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
    dead_lettered_at: datetime | None = None,
    last_error: str | None = None,
) -> Job:
    return Job(
        id=job_id,
        queue=queue,
        job_type=job_type,
        payload_json={},
        status=job_status,
        priority=0,
        attempts=3,
        max_retries=3,
        next_run_at=created_at,
        timeout_seconds=60,
        created_at=created_at,
        updated_at=created_at,
        failed_at=dead_lettered_at,
        dead_lettered_at=dead_lettered_at,
        last_error=last_error,
    )


@pytest.fixture
async def seeded_dead_letter_jobs(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> list[Job]:
    _http_client, sessionmaker = live_client
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    jobs = [
        _job(
            queue="emails",
            job_type="send_email",
            job_status=JobStatus.DEAD_LETTER.value,
            created_at=base_time,
            dead_lettered_at=base_time,
            last_error="max retries exceeded",
        ),
        _job(
            queue="emails",
            job_type="process_image",
            job_status=JobStatus.DEAD_LETTER.value,
            created_at=base_time + timedelta(minutes=1),
            dead_lettered_at=base_time + timedelta(minutes=5),
            last_error="handler crashed",
        ),
        _job(
            queue="reports",
            job_type="generate_report",
            job_status=JobStatus.DEAD_LETTER.value,
            created_at=base_time + timedelta(minutes=2),
            dead_lettered_at=base_time + timedelta(minutes=10),
            last_error="lease expired",
        ),
        _job(
            queue="emails",
            job_type="send_email",
            job_status=JobStatus.QUEUED.value,
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


async def test_list_dead_letter_jobs_returns_only_dead_letter_status(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_dead_letter_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/dead-letter")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert len(body["jobs"]) == 3
    assert all(job["status"] == "dead_letter" for job in body["jobs"])
    returned_ids = {job["job_id"] for job in body["jobs"]}
    dead_letter_ids = {
        job.id
        for job in seeded_dead_letter_jobs
        if job.status == JobStatus.DEAD_LETTER.value
    }
    assert returned_ids == dead_letter_ids


async def test_list_dead_letter_jobs_includes_last_error(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_dead_letter_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/dead-letter")

    assert response.status_code == 200
    jobs_by_id = {job["job_id"]: job for job in response.json()["jobs"]}
    dead_letter_jobs = [
        job
        for job in seeded_dead_letter_jobs
        if job.status == JobStatus.DEAD_LETTER.value
    ]
    for job in dead_letter_jobs:
        assert jobs_by_id[job.id]["last_error"] == job.last_error


async def test_list_dead_letter_jobs_filters_by_queue_and_job_type(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_dead_letter_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/dead-letter?queue=emails&job_type=send_email")

    assert response.status_code == 200
    body = response.json()
    expected_job = next(
        job
        for job in seeded_dead_letter_jobs
        if job.queue == "emails"
        and job.job_type == "send_email"
        and job.status == JobStatus.DEAD_LETTER.value
    )
    assert body["total"] == 1
    assert len(body["jobs"]) == 1
    assert body["jobs"][0]["job_id"] == expected_job.id
    assert body["jobs"][0]["queue"] == "emails"
    assert body["jobs"][0]["job_type"] == "send_email"


async def test_list_dead_letter_jobs_orders_by_dead_lettered_at_desc_then_id_desc(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_dead_letter_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/dead-letter")

    assert response.status_code == 200
    returned_ids = [job["job_id"] for job in response.json()["jobs"]]
    dead_letter_jobs = [
        job
        for job in seeded_dead_letter_jobs
        if job.status == JobStatus.DEAD_LETTER.value
    ]
    jobs_by_id = {job.id: job for job in dead_letter_jobs}
    expected_ids = sorted(
        (job.id for job in dead_letter_jobs),
        key=lambda job_id: (jobs_by_id[job_id].dead_lettered_at, job_id),
        reverse=True,
    )
    assert returned_ids == expected_ids


async def test_list_dead_letter_jobs_paginates_with_total_independent_of_limit(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_dead_letter_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/dead-letter?limit=2&offset=1")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["limit"] == 2
    assert body["offset"] == 1
    assert len(body["jobs"]) == 2
