import asyncio
from datetime import UTC, datetime, timedelta

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
    attempts: int = 3,
    dead_lettered_at: datetime | None = None,
    failed_at: datetime | None = None,
    last_error: str | None = None,
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
        created_at=created_at,
        updated_at=created_at,
        failed_at=failed_at,
        dead_lettered_at=dead_lettered_at,
        last_error=last_error,
        locked_by=locked_by,
        locked_at=locked_at,
        lease_expires_at=lease_expires_at,
    )


async def _seed_job(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    job_status: str,
    attempts: int = 3,
    last_error: str | None = None,
    failed_at: datetime | None = None,
    dead_lettered_at: datetime | None = None,
    locked_by: str | None = None,
    locked_at: datetime | None = None,
    lease_expires_at: datetime | None = None,
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
        locked_by=locked_by,
        locked_at=locked_at,
        lease_expires_at=lease_expires_at,
    )
    async with sessionmaker() as session:
        session.add(job)
        await session.commit()
        await session.refresh(job)
    return job


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
            failed_at=base_time,
            last_error="max retries exceeded",
        ),
        _job(
            queue="emails",
            job_type="process_image",
            job_status=JobStatus.DEAD_LETTER.value,
            created_at=base_time + timedelta(minutes=1),
            dead_lettered_at=base_time + timedelta(minutes=5),
            failed_at=base_time + timedelta(minutes=5),
            last_error="handler crashed",
        ),
        _job(
            queue="reports",
            job_type="generate_report",
            job_status=JobStatus.DEAD_LETTER.value,
            created_at=base_time + timedelta(minutes=2),
            dead_lettered_at=base_time + timedelta(minutes=10),
            failed_at=base_time + timedelta(minutes=10),
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


async def test_retry_dead_letter_job_resets_and_requeues(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    dead_lettered_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    job = await _seed_job(
        sessionmaker,
        job_status=JobStatus.DEAD_LETTER.value,
        attempts=3,
        last_error="max retries exceeded",
        failed_at=dead_lettered_at,
        dead_lettered_at=dead_lettered_at,
    )

    response = await http_client.post(f"/dead-letter/{job.id}/retry")

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == job.id
    assert body["status"] == "queued"
    assert body["attempts"] == 0
    assert body["last_error"] is None
    assert body["failed_at"] is None
    assert body["dead_lettered_at"] is None


async def test_retry_dead_letter_job_persists_to_db(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    dead_lettered_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    job = await _seed_job(
        sessionmaker,
        job_status=JobStatus.DEAD_LETTER.value,
        attempts=3,
        last_error="max retries exceeded",
        failed_at=dead_lettered_at,
        dead_lettered_at=dead_lettered_at,
        locked_by="worker-1",
        locked_at=dead_lettered_at,
        lease_expires_at=dead_lettered_at,
    )

    response = await http_client.post(f"/dead-letter/{job.id}/retry")

    assert response.status_code == 200

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


async def test_retry_dead_letter_job_returns_404_for_missing_job(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.post("/dead-letter/999999/retry")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}


@pytest.mark.parametrize(
    "job_status",
    [
        JobStatus.FAILED.value,
        JobStatus.QUEUED.value,
        JobStatus.SCHEDULED.value,
        JobStatus.RUNNING.value,
        JobStatus.SUCCEEDED.value,
        JobStatus.RETRYING.value,
        JobStatus.CANCELLED.value,
    ],
)
async def test_retry_dead_letter_job_returns_409_for_non_dead_letter_statuses(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    job_status: str,
) -> None:
    http_client, sessionmaker = live_client
    created_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    job = await _seed_job(
        sessionmaker,
        job_status=job_status,
        failed_at=created_at if job_status == JobStatus.FAILED.value else None,
    )

    response = await http_client.post(f"/dead-letter/{job.id}/retry")

    assert response.status_code == 409
    assert response.json() == {"detail": "Job is not in dead_letter status"}


async def test_retry_dead_letter_job_is_atomic_under_concurrent_requests(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    dead_lettered_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    job = await _seed_job(
        sessionmaker,
        job_status=JobStatus.DEAD_LETTER.value,
        attempts=3,
        last_error="max retries exceeded",
        failed_at=dead_lettered_at,
        dead_lettered_at=dead_lettered_at,
    )

    response_one, response_two = await asyncio.gather(
        http_client.post(f"/dead-letter/{job.id}/retry"),
        http_client.post(f"/dead-letter/{job.id}/retry"),
    )

    status_codes = sorted([response_one.status_code, response_two.status_code])
    assert status_codes == [200, 409]

    async with sessionmaker() as session:
        persisted = (
            await session.execute(select(Job).where(Job.id == job.id))
        ).scalar_one()
        assert persisted.status == JobStatus.QUEUED.value
        assert persisted.attempts == 0
