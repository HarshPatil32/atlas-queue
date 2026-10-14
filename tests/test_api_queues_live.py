from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.models import Job, JobStatus
from core.schemas import DEFAULT_LIMIT
from tests.queue_stats_helpers import zero_status_counts


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


async def test_get_queue_stats_per_status_counts_and_depth(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/queues/emails/stats")

    assert response.status_code == 200
    body = response.json()
    expected_counts = zero_status_counts()
    expected_counts[JobStatus.QUEUED.value] = 2
    expected_counts[JobStatus.RUNNING.value] = 1
    assert body["queue"] == "emails"
    assert body["depth"] == 2
    assert body["status_counts"] == expected_counts
    oldest = body["oldest_job"]
    assert oldest is not None
    assert oldest["job_type"] == "send_email"
    assert oldest["status"] == JobStatus.QUEUED.value


async def test_get_queue_stats_terminal_only(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/queues/reports/stats")

    assert response.status_code == 200
    body = response.json()
    expected_counts = zero_status_counts()
    expected_counts[JobStatus.SUCCEEDED.value] = 1
    expected_counts[JobStatus.FAILED.value] = 1
    assert body["depth"] == 0
    assert body["oldest_job"] is None
    assert body["status_counts"] == expected_counts


async def test_get_queue_stats_not_found(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/queues/unknown/stats")

    assert response.status_code == 404
    assert response.json() == {"detail": "Queue not found"}


async def test_get_queue_stats_oldest_job_among_depth_statuses(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, sessionmaker = live_client
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    async with sessionmaker() as session:
        session.add(
            _job(
                queue="emails",
                job_type="older_running",
                job_status=JobStatus.RUNNING.value,
                created_at=base_time - timedelta(hours=1),
            )
        )
        await session.commit()

    response = await http_client.get("/queues/emails/stats")

    assert response.status_code == 200
    oldest = response.json()["oldest_job"]
    assert oldest is not None
    assert oldest["job_type"] == "send_email"
    assert oldest["status"] == JobStatus.QUEUED.value
    assert oldest["created_at"] == base_time.isoformat().replace("+00:00", "Z")


async def test_get_queue_stats_oldest_job_tie_breaks_on_id(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    earlier = datetime(2026, 2, 1, 9, 0, 0, tzinfo=UTC)
    tied_time = datetime(2026, 2, 1, 10, 0, 0, tzinfo=UTC)
    later = datetime(2026, 2, 1, 11, 0, 0, tzinfo=UTC)
    async with sessionmaker() as session:
        loser = _job(
            queue="tie",
            job_type="loser",
            job_status=JobStatus.QUEUED.value,
            created_at=earlier,
        )
        winner = _job(
            queue="tie",
            job_type="winner",
            job_status=JobStatus.QUEUED.value,
            created_at=later,
        )
        session.add_all([loser, winner])
        await session.commit()
        await session.refresh(loser)
        await session.refresh(winner)
        loser.created_at = tied_time
        loser.updated_at = tied_time
        await session.commit()
        await session.refresh(loser)

    assert loser.id < winner.id

    response = await http_client.get("/queues/tie/stats")

    assert response.status_code == 200
    oldest = response.json()["oldest_job"]
    assert oldest is not None
    assert oldest["job_id"] == loser.id
    assert oldest["job_type"] == "loser"


async def test_get_queue_stats_isolated_per_queue(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
    seeded_jobs: list[Job],
) -> None:
    http_client, _sessionmaker = live_client

    response = await http_client.get("/queues/emails/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["queue"] == "emails"
    assert body["depth"] == 2
    assert body["status_counts"][JobStatus.SUCCEEDED.value] == 0


async def test_get_queue_stats_scheduled_future_counts_as_scheduled(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    now = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
    future = now + timedelta(days=1)
    job = Job(
        queue="sched",
        job_type="delayed",
        payload_json={},
        status=JobStatus.SCHEDULED.value,
        priority=0,
        attempts=0,
        max_retries=3,
        next_run_at=future,
        timeout_seconds=60,
        created_at=now,
        updated_at=now,
    )
    async with sessionmaker() as session:
        session.add(job)
        await session.commit()
        await session.refresh(job)

    response = await http_client.get("/queues/sched/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["depth"] == 1
    assert body["status_counts"][JobStatus.SCHEDULED.value] == 1
    assert body["status_counts"][JobStatus.QUEUED.value] == 0
    oldest = body["oldest_job"]
    assert oldest is not None
    assert oldest["status"] == JobStatus.SCHEDULED.value
    assert oldest["run_at"] == future.isoformat().replace("+00:00", "Z")


async def test_get_queue_stats_due_scheduled_reports_raw_status(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    past_run = datetime(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
    created = datetime(2026, 4, 1, 8, 0, 0, tzinfo=UTC)
    job = Job(
        queue="due-sched",
        job_type="was_scheduled",
        payload_json={},
        status=JobStatus.SCHEDULED.value,
        priority=0,
        attempts=0,
        max_retries=3,
        next_run_at=past_run,
        timeout_seconds=60,
        created_at=created,
        updated_at=created,
    )
    async with sessionmaker() as session:
        session.add(job)
        await session.commit()

    response = await http_client.get("/queues/due-sched/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["depth"] == 1
    assert body["status_counts"][JobStatus.SCHEDULED.value] == 1
    assert body["status_counts"][JobStatus.QUEUED.value] == 0
    oldest = body["oldest_job"]
    assert oldest is not None
    assert oldest["status"] == JobStatus.SCHEDULED.value


async def test_get_queue_stats_retrying_in_depth_and_oldest(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    base = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
    async with sessionmaker() as session:
        session.add_all(
            [
                _job(
                    queue="retry-q",
                    job_type="retrying_job",
                    job_status=JobStatus.RETRYING.value,
                    created_at=base,
                ),
                _job(
                    queue="retry-q",
                    job_type="queued_later",
                    job_status=JobStatus.QUEUED.value,
                    created_at=base + timedelta(hours=1),
                ),
            ]
        )
        await session.commit()

    response = await http_client.get("/queues/retry-q/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["depth"] == 2
    assert body["status_counts"][JobStatus.RETRYING.value] == 1
    assert body["status_counts"][JobStatus.QUEUED.value] == 1
    oldest = body["oldest_job"]
    assert oldest is not None
    assert oldest["job_type"] == "retrying_job"
    assert oldest["status"] == JobStatus.RETRYING.value


async def test_get_queue_stats_dead_letter_and_cancelled_counts(
    live_client: tuple[AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    http_client, sessionmaker = live_client
    base = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    async with sessionmaker() as session:
        session.add_all(
            [
                _job(
                    queue="terminal-mix",
                    job_type="dead",
                    job_status=JobStatus.DEAD_LETTER.value,
                    created_at=base,
                ),
                _job(
                    queue="terminal-mix",
                    job_type="cancelled",
                    job_status=JobStatus.CANCELLED.value,
                    created_at=base + timedelta(minutes=1),
                ),
            ]
        )
        await session.commit()

    response = await http_client.get("/queues/terminal-mix/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["depth"] == 0
    assert body["oldest_job"] is None
    assert body["status_counts"][JobStatus.DEAD_LETTER.value] == 1
    assert body["status_counts"][JobStatus.CANCELLED.value] == 1
