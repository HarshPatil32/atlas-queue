import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.backoff import retry_delay_seconds
from core.config import get_settings
from core.db import Base, dispose_engine, get_engine, get_sessionmaker
from core.models import Job, JobAttempt, JobAttemptStatus, JobStatus
from tests.live_postgres import drop_metadata_tables, require_live_postgres_async
from worker.claim import (
    _CLAIMABLE_STATUSES,
    _SHUTDOWN_ATTEMPT_MESSAGE,
    claim_jobs,
    count_in_flight_jobs,
    extend_lease,
    reap_expired_jobs,
    release_in_flight_jobs,
)

DEFAULT_LEASE_SECONDS = 30


@pytest.fixture
async def live_claim_db(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    live_url = await require_live_postgres_async()

    monkeypatch.setenv("DATABASE_URL", live_url)
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_sessionmaker.cache_clear()

    engine = get_engine()
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    yield get_sessionmaker()

    await drop_metadata_tables(engine)
    await dispose_engine()
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_sessionmaker.cache_clear()


async def _insert_job(
    session: AsyncSession,
    *,
    queue: str = "default",
    status: str = JobStatus.QUEUED.value,
    priority: int = 0,
    next_run_at: datetime | None = None,
    created_at: datetime | None = None,
    job_type: str = "test",
    attempts: int = 0,
    max_retries: int = 3,
    payload_json: dict | None = None,
    idempotency_key: str | None = None,
    timeout_seconds: int = 60,
) -> Job:
    now = datetime.now(UTC)
    job = Job(
        queue=queue,
        job_type=job_type,
        payload_json=payload_json if payload_json is not None else {},
        status=status,
        priority=priority,
        attempts=attempts,
        max_retries=max_retries,
        next_run_at=next_run_at or now,
        created_at=created_at or now,
        idempotency_key=idempotency_key,
        timeout_seconds=timeout_seconds,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def _insert_running_job(
    session: AsyncSession,
    *,
    worker_name: str,
    queue: str = "default",
    lease_expires_at: datetime | None = None,
    attempts: int = 1,
    max_retries: int = 3,
    priority: int = 0,
    payload_json: dict | None = None,
    idempotency_key: str | None = None,
    timeout_seconds: int = 60,
    job_type: str = "test",
    created_at: datetime | None = None,
    last_error: str | None = None,
    failed_at: datetime | None = None,
) -> Job:
    now = datetime.now(UTC)
    job = Job(
        queue=queue,
        job_type=job_type,
        payload_json=payload_json if payload_json is not None else {},
        status=JobStatus.RUNNING.value,
        priority=priority,
        next_run_at=now,
        created_at=created_at or now,
        locked_by=worker_name,
        locked_at=now,
        lease_expires_at=(
            lease_expires_at or now + timedelta(seconds=DEFAULT_LEASE_SECONDS)
        ),
        attempts=attempts,
        max_retries=max_retries,
        idempotency_key=idempotency_key,
        timeout_seconds=timeout_seconds,
        last_error=last_error,
        failed_at=failed_at,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def test_count_in_flight_jobs_counts_running_jobs_for_worker(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        await _insert_running_job(session, worker_name="worker-1")
        await _insert_running_job(session, worker_name="worker-1")
        await _insert_running_job(session, worker_name="worker-2")

    async with live_claim_db() as session:
        count = await count_in_flight_jobs(session, worker_name="worker-1")

    assert count == 2


async def test_count_in_flight_jobs_excludes_other_workers(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        await _insert_running_job(session, worker_name="worker-2")

    async with live_claim_db() as session:
        count = await count_in_flight_jobs(session, worker_name="worker-1")

    assert count == 0


async def test_count_in_flight_jobs_excludes_expired_leases(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    async with live_claim_db() as session:
        await _insert_running_job(
            session,
            worker_name="worker-1",
            lease_expires_at=expired,
        )

    async with live_claim_db() as session:
        count = await count_in_flight_jobs(session, worker_name="worker-1")

    assert count == 0


async def test_count_in_flight_jobs_returns_zero_when_none(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        count = await count_in_flight_jobs(session, worker_name="worker-1")

    assert count == 0


async def test_claim_jobs_orders_by_priority_next_run_at_created_at(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    async with live_claim_db() as session:
        job_low = await _insert_job(
            session,
            priority=1,
            next_run_at=base + timedelta(minutes=2),
            created_at=base,
        )
        job_high = await _insert_job(
            session,
            priority=10,
            next_run_at=base + timedelta(minutes=1),
            created_at=base + timedelta(seconds=1),
        )
        # Same priority and next_run_at; created_at ASC breaks the tie.
        job_tie_earlier_created = await _insert_job(
            session,
            priority=10,
            next_run_at=base,
            created_at=base + timedelta(seconds=2),
        )
        job_tie_later_created = await _insert_job(
            session,
            priority=10,
            next_run_at=base,
            created_at=base + timedelta(seconds=3),
        )

    async with live_claim_db() as session:
        claimed = await claim_jobs(
            session,
            queues=["default"],
            worker_name="worker-1",
            limit=10,
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert [job.id for job in claimed] == [
        job_tie_earlier_created.id,
        job_tie_later_created.id,
        job_high.id,
        job_low.id,
    ]


async def test_claim_jobs_skips_non_claimable_statuses(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        claimable = await _insert_job(session, status=JobStatus.QUEUED.value)
        for status in JobStatus:
            if status.value in _CLAIMABLE_STATUSES:
                continue
            await _insert_job(session, status=status.value)

    async with live_claim_db() as session:
        claimed = await claim_jobs(
            session,
            queues=["default"],
            worker_name="worker-1",
            limit=10,
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert len(claimed) == 1
    assert claimed[0].id == claimable.id


async def test_claim_jobs_skips_jobs_not_yet_due(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    future = datetime.now(UTC) + timedelta(hours=1)
    async with live_claim_db() as session:
        due = await _insert_job(session, next_run_at=datetime.now(UTC))
        await _insert_job(session, next_run_at=future)

    async with live_claim_db() as session:
        claimed = await claim_jobs(
            session,
            queues=["default"],
            worker_name="worker-1",
            limit=10,
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert len(claimed) == 1
    assert claimed[0].id == due.id


async def test_claim_jobs_claims_job_at_exact_next_run_at(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        db_now = await session.scalar(select(func.now()))
        assert db_now is not None
        at_now = Job(
            queue="default",
            job_type="test",
            payload_json={},
            status=JobStatus.QUEUED.value,
            priority=0,
            next_run_at=db_now,
            created_at=db_now,
        )
        session.add(at_now)
        # flush (not commit) keeps the transaction open so func.now() is stable
        await session.flush()

        claimed = await claim_jobs(
            session,
            queues=["default"],
            worker_name="worker-1",
            limit=10,
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert len(claimed) == 1
    assert claimed[0].id == at_now.id


async def test_claim_jobs_filters_by_queue(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        in_queue = await _insert_job(session, queue="alpha")
        await _insert_job(session, queue="beta")

    async with live_claim_db() as session:
        claimed = await claim_jobs(
            session,
            queues=["alpha"],
            worker_name="worker-1",
            limit=10,
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert len(claimed) == 1
    assert claimed[0].id == in_queue.id


async def test_claim_jobs_respects_limit(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        for _ in range(5):
            await _insert_job(session)

    async with live_claim_db() as session:
        claimed = await claim_jobs(
            session,
            queues=["default"],
            worker_name="worker-1",
            limit=2,
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert len(claimed) == 2

    async with live_claim_db() as session:
        remaining = (
            await session.scalars(
                select(Job).where(Job.status == JobStatus.QUEUED.value)
            )
        ).all()

    assert len(remaining) == 3


async def test_claim_jobs_mutates_job_state(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        job = await _insert_job(session)
        assert job.attempts == 0

    async with live_claim_db() as session:
        claimed = await claim_jobs(
            session,
            queues=["default"],
            worker_name="worker-1",
            limit=1,
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert len(claimed) == 1
    claimed_job = claimed[0]
    assert claimed_job.status == JobStatus.RUNNING.value
    assert claimed_job.locked_by == "worker-1"
    assert claimed_job.locked_at is not None
    assert claimed_job.lease_expires_at is not None
    assert claimed_job.attempts == 1
    assert (
        claimed_job.lease_expires_at - claimed_job.locked_at
    ).total_seconds() == pytest.approx(DEFAULT_LEASE_SECONDS, abs=2)


async def test_claim_jobs_reclaims_retrying_job_and_preserves_state(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    payload = {"foo": "bar"}
    created_at = datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC)
    async with live_claim_db() as session:
        job = await _insert_job(
            session,
            queue="reports",
            status=JobStatus.RETRYING.value,
            priority=7,
            attempts=1,
            max_retries=5,
            payload_json=payload,
            idempotency_key="retry-key-1",
            timeout_seconds=120,
            created_at=created_at,
        )

    async with live_claim_db() as session:
        claimed = await claim_jobs(
            session,
            queues=["reports"],
            worker_name="worker-1",
            limit=1,
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert len(claimed) == 1
    claimed_job = claimed[0]
    assert claimed_job.status == JobStatus.RUNNING.value
    assert claimed_job.locked_by == "worker-1"
    assert claimed_job.locked_at is not None
    assert claimed_job.lease_expires_at is not None
    assert claimed_job.attempts == 2
    assert claimed_job.payload_json == payload
    assert claimed_job.queue == "reports"
    assert claimed_job.priority == 7
    assert claimed_job.max_retries == 5
    assert claimed_job.idempotency_key == "retry-key-1"
    assert claimed_job.timeout_seconds == 120
    assert claimed_job.created_at == created_at
    assert claimed_job.id == job.id


async def test_claim_jobs_returns_empty_when_limit_is_zero(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        await _insert_job(session)

    async with live_claim_db() as session:
        claimed = await claim_jobs(
            session,
            queues=["default"],
            worker_name="worker-1",
            limit=0,
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert claimed == []


async def test_claim_jobs_returns_empty_when_queues_is_empty(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        await _insert_job(session)

    async with live_claim_db() as session:
        claimed = await claim_jobs(
            session,
            queues=[],
            worker_name="worker-1",
            limit=10,
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert claimed == []


async def test_claim_jobs_rejects_non_positive_lease_seconds(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        await _insert_job(session)

    async with live_claim_db() as session:
        with pytest.raises(ValueError, match="lease_seconds must be > 0"):
            await claim_jobs(
                session,
                queues=["default"],
                worker_name="worker-1",
                limit=1,
                lease_seconds=0,
            )


async def test_claim_jobs_skips_locked_row(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        locked_job = await _insert_job(session, priority=10)
        other_job = await _insert_job(session, priority=1)

    lock_ready = asyncio.Event()
    release_lock = asyncio.Event()

    async def hold_lock() -> None:
        async with live_claim_db() as session:
            await session.execute(
                select(Job.id)
                .where(Job.id == locked_job.id)
                .with_for_update(skip_locked=True)
            )
            lock_ready.set()
            await release_lock.wait()
            await session.rollback()

    async def claim_while_locked() -> list[Job]:
        await lock_ready.wait()
        async with live_claim_db() as session:
            return await claim_jobs(
                session,
                queues=["default"],
                worker_name="worker-2",
                limit=10,
                lease_seconds=DEFAULT_LEASE_SECONDS,
            )

    holder = asyncio.create_task(hold_lock())
    try:
        claimed = await asyncio.wait_for(claim_while_locked(), timeout=5.0)
    finally:
        release_lock.set()
        await holder

    assert len(claimed) == 1
    assert claimed[0].id == other_job.id


async def test_concurrent_claim_jobs_do_not_double_claim(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        jobs = [await _insert_job(session) for _ in range(4)]

    async def claim_batch(worker_name: str) -> list[Job]:
        async with live_claim_db() as session:
            return await claim_jobs(
                session,
                queues=["default"],
                worker_name=worker_name,
                limit=4,
                lease_seconds=DEFAULT_LEASE_SECONDS,
            )

    first_batch, second_batch = await asyncio.gather(
        claim_batch("worker-a"),
        claim_batch("worker-b"),
    )

    claimed_ids = {job.id for job in first_batch + second_batch}
    assert claimed_ids == {job.id for job in jobs}
    assert len(claimed_ids) == len(jobs)


async def test_release_in_flight_jobs_returns_running_jobs_to_queued(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        job_one = await _insert_running_job(session, worker_name="worker-1")
        job_two = await _insert_running_job(session, worker_name="worker-1")

    async with live_claim_db() as session:
        released = await release_in_flight_jobs(session, worker_name="worker-1")

    assert released == 2

    async with live_claim_db() as session:
        released_one = await session.scalar(select(Job).where(Job.id == job_one.id))
        released_two = await session.scalar(select(Job).where(Job.id == job_two.id))

    assert released_one is not None
    assert released_one.status == JobStatus.QUEUED.value
    assert released_one.attempts == 0
    assert released_one.locked_by is None
    assert released_one.locked_at is None
    assert released_one.lease_expires_at is None

    assert released_two is not None
    assert released_two.status == JobStatus.QUEUED.value
    assert released_two.attempts == 0
    assert released_two.locked_by is None
    assert released_two.locked_at is None
    assert released_two.lease_expires_at is None

    async with live_claim_db() as session:
        cancelled_attempts = (
            await session.scalars(
                select(JobAttempt).where(
                    JobAttempt.job_id.in_([job_one.id, job_two.id]),
                    JobAttempt.status == JobAttemptStatus.CANCELLED.value,
                )
            )
        ).all()

    assert len(cancelled_attempts) == 2


async def test_release_in_flight_jobs_ignores_other_workers(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        other_worker_job = await _insert_running_job(session, worker_name="worker-2")
        await _insert_running_job(session, worker_name="worker-1")

    async with live_claim_db() as session:
        released = await release_in_flight_jobs(session, worker_name="worker-1")

    assert released == 1

    async with live_claim_db() as session:
        other_job = await session.scalar(
            select(Job).where(Job.id == other_worker_job.id)
        )

    assert other_job is not None
    assert other_job.status == JobStatus.RUNNING.value
    assert other_job.locked_by == "worker-2"


async def test_release_in_flight_jobs_ignores_non_running_statuses(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        queued = await _insert_job(session, status=JobStatus.QUEUED.value)
        await _insert_running_job(session, worker_name="worker-1")

    async with live_claim_db() as session:
        released = await release_in_flight_jobs(session, worker_name="worker-1")

    assert released == 1

    async with live_claim_db() as session:
        unchanged = await session.scalar(select(Job).where(Job.id == queued.id))

    assert unchanged is not None
    assert unchanged.status == JobStatus.QUEUED.value
    assert unchanged.locked_by is None


async def test_release_in_flight_jobs_returns_zero_when_none(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        released = await release_in_flight_jobs(session, worker_name="worker-1")

    assert released == 0


async def test_release_in_flight_jobs_decrements_attempts(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        job = await _insert_running_job(
            session,
            worker_name="worker-1",
            attempts=2,
        )

    async with live_claim_db() as session:
        released = await release_in_flight_jobs(session, worker_name="worker-1")

    assert released == 1

    async with live_claim_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))

    assert updated is not None
    assert updated.attempts == 1


async def test_release_in_flight_jobs_preserves_last_error_and_failed_at(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    stale_failed_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    async with live_claim_db() as session:
        job = await _insert_running_job(
            session,
            worker_name="worker-1",
            attempts=2,
            last_error="prior failure",
            failed_at=stale_failed_at,
        )

    async with live_claim_db() as session:
        released = await release_in_flight_jobs(session, worker_name="worker-1")

    assert released == 1

    async with live_claim_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))

    assert updated is not None
    assert updated.last_error == "prior failure"
    assert updated.failed_at == stale_failed_at


async def test_release_in_flight_jobs_inserts_cancelled_job_attempt(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        job = await _insert_running_job(
            session,
            worker_name="worker-1",
            attempts=2,
        )

    async with live_claim_db() as session:
        released = await release_in_flight_jobs(session, worker_name="worker-1")

    assert released == 1

    async with live_claim_db() as session:
        attempt = await session.scalar(
            select(JobAttempt).where(
                JobAttempt.job_id == job.id,
                JobAttempt.attempt_number == 2,
            )
        )

    assert attempt is not None
    assert attempt.status == JobAttemptStatus.CANCELLED.value
    assert attempt.worker_id == "worker-1"
    assert attempt.error_message == _SHUTDOWN_ATTEMPT_MESSAGE
    assert attempt.finished_at is not None


async def test_reap_expired_jobs_clears_stale_failed_at_when_retrying(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    stale_failed_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    async with live_claim_db() as session:
        job = await _insert_running_job(
            session,
            worker_name="dead-worker",
            lease_expires_at=expired,
            attempts=1,
            max_retries=3,
            failed_at=stale_failed_at,
        )

    async with live_claim_db() as session:
        reaped = await reap_expired_jobs(session)

    assert reaped == 1

    async with live_claim_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))

    assert updated is not None
    assert updated.status == JobStatus.RETRYING.value
    assert updated.failed_at is None


async def test_extend_lease_extends_lease_expires_at(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        job = await _insert_running_job(session, worker_name="worker-1")
        original_expiry = job.lease_expires_at
        assert original_expiry is not None

    async with live_claim_db() as session:
        extended = await extend_lease(
            session,
            job_id=job.id,
            worker_name="worker-1",
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert extended is True

    async with live_claim_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))

    assert updated is not None
    assert updated.lease_expires_at is not None
    assert updated.lease_expires_at > original_expiry


async def test_extend_lease_ignores_job_owned_by_other_worker(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        job = await _insert_running_job(session, worker_name="worker-1")
        original_expiry = job.lease_expires_at

    async with live_claim_db() as session:
        extended = await extend_lease(
            session,
            job_id=job.id,
            worker_name="worker-2",
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert extended is False

    async with live_claim_db() as session:
        unchanged = await session.scalar(select(Job).where(Job.id == job.id))

    assert unchanged is not None
    assert unchanged.lease_expires_at == original_expiry


async def test_extend_lease_ignores_non_running_job(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        job = await _insert_job(session, status=JobStatus.QUEUED.value)

    async with live_claim_db() as session:
        extended = await extend_lease(
            session,
            job_id=job.id,
            worker_name="worker-1",
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert extended is False


async def test_extend_lease_ignores_expired_lease(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    async with live_claim_db() as session:
        job = await _insert_running_job(
            session,
            worker_name="worker-1",
            lease_expires_at=expired,
        )
        original_expiry = job.lease_expires_at

    async with live_claim_db() as session:
        extended = await extend_lease(
            session,
            job_id=job.id,
            worker_name="worker-1",
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert extended is False

    async with live_claim_db() as session:
        unchanged = await session.scalar(select(Job).where(Job.id == job.id))

    assert unchanged is not None
    assert unchanged.lease_expires_at == original_expiry


async def test_extend_lease_returns_false_for_missing_job(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        extended = await extend_lease(
            session,
            job_id=999999,
            worker_name="worker-1",
            lease_seconds=DEFAULT_LEASE_SECONDS,
        )

    assert extended is False


async def test_extend_lease_rejects_non_positive_lease_seconds(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        job = await _insert_running_job(session, worker_name="worker-1")

    async with live_claim_db() as session:
        with pytest.raises(ValueError, match="lease_seconds must be > 0"):
            await extend_lease(
                session,
                job_id=job.id,
                worker_name="worker-1",
                lease_seconds=0,
            )


async def test_reap_expired_jobs_retries_job_under_max_retries(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    async with live_claim_db() as session:
        job = await _insert_running_job(
            session,
            worker_name="dead-worker",
            lease_expires_at=expired,
            attempts=1,
            max_retries=3,
        )

    async with live_claim_db() as session:
        reaped = await reap_expired_jobs(session)

    assert reaped == 1

    async with live_claim_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))
        attempt = await session.scalar(
            select(JobAttempt).where(
                JobAttempt.job_id == job.id,
                JobAttempt.attempt_number == 1,
            )
        )

    assert updated is not None
    assert updated.status == JobStatus.RETRYING.value
    assert updated.locked_by is None
    assert updated.locked_at is None
    assert updated.lease_expires_at is None
    assert updated.last_error == "lease expired: worker did not renew in time"
    assert updated.dead_lettered_at is None
    assert updated.next_run_at is not None
    assert attempt is not None
    assert attempt.status == JobAttemptStatus.FAILED.value
    assert attempt.worker_id == "dead-worker"


async def test_reap_expired_jobs_does_not_double_increment_attempts_and_preserves_state(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    payload = {"task": "reap-me"}
    created_at = datetime(2026, 2, 1, 8, 30, 0, tzinfo=UTC)
    async with live_claim_db() as session:
        job = await _insert_running_job(
            session,
            worker_name="dead-worker",
            queue="emails",
            lease_expires_at=expired,
            attempts=1,
            max_retries=3,
            priority=9,
            payload_json=payload,
            idempotency_key="reap-key-1",
            timeout_seconds=90,
            created_at=created_at,
        )

    async with live_claim_db() as session:
        reaped = await reap_expired_jobs(session)

    assert reaped == 1

    async with live_claim_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))

    assert updated is not None
    assert updated.attempts == 1
    assert updated.status == JobStatus.RETRYING.value
    assert updated.locked_by is None
    assert updated.locked_at is None
    assert updated.lease_expires_at is None
    assert updated.next_run_at is not None
    assert (updated.next_run_at - updated.updated_at).total_seconds() == pytest.approx(
        retry_delay_seconds(1), abs=2
    )
    assert updated.payload_json == payload
    assert updated.queue == "emails"
    assert updated.priority == 9
    assert updated.max_retries == 3
    assert updated.idempotency_key == "reap-key-1"
    assert updated.timeout_seconds == 90
    assert updated.created_at == created_at


async def test_reap_expired_jobs_marks_dead_letter_at_max_retries(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    async with live_claim_db() as session:
        job = await _insert_running_job(
            session,
            worker_name="dead-worker",
            lease_expires_at=expired,
            attempts=3,
            max_retries=3,
        )
        original_next_run_at = job.next_run_at

    async with live_claim_db() as session:
        reaped = await reap_expired_jobs(session)

    assert reaped == 1

    async with live_claim_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))
        attempt = await session.scalar(
            select(JobAttempt).where(
                JobAttempt.job_id == job.id,
                JobAttempt.attempt_number == 3,
            )
        )

    assert updated is not None
    assert updated.status == JobStatus.DEAD_LETTER.value
    assert updated.failed_at is not None
    assert updated.dead_lettered_at is not None
    assert updated.next_run_at == original_next_run_at
    assert attempt is not None
    assert attempt.status == JobAttemptStatus.FAILED.value
    assert attempt.worker_id == "dead-worker"


async def test_reap_expired_jobs_at_max_retries_preserves_state(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    payload = {"task": "terminal-reap"}
    created_at = datetime(2026, 2, 2, 12, 0, 0, tzinfo=UTC)
    async with live_claim_db() as session:
        job = await _insert_running_job(
            session,
            worker_name="dead-worker",
            queue="market-data",
            lease_expires_at=expired,
            attempts=3,
            max_retries=3,
            priority=4,
            payload_json=payload,
            idempotency_key="reap-key-terminal",
            timeout_seconds=45,
            created_at=created_at,
        )
        original_next_run_at = job.next_run_at

    async with live_claim_db() as session:
        reaped = await reap_expired_jobs(session)

    assert reaped == 1

    async with live_claim_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))

    assert updated is not None
    assert updated.attempts == 3
    assert updated.status == JobStatus.DEAD_LETTER.value
    assert updated.failed_at is not None
    assert updated.dead_lettered_at is not None
    assert updated.next_run_at == original_next_run_at
    assert updated.locked_by is None
    assert updated.locked_at is None
    assert updated.lease_expires_at is None
    assert updated.payload_json == payload
    assert updated.queue == "market-data"
    assert updated.priority == 4
    assert updated.max_retries == 3
    assert updated.idempotency_key == "reap-key-terminal"
    assert updated.timeout_seconds == 45
    assert updated.created_at == created_at


async def test_reap_expired_jobs_ignores_non_expired_leases(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        await _insert_running_job(session, worker_name="worker-1")

    async with live_claim_db() as session:
        reaped = await reap_expired_jobs(session)

    assert reaped == 0


async def test_reap_expired_jobs_only_touches_running_status(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    async with live_claim_db() as session:
        queued = await _insert_job(session, status=JobStatus.QUEUED.value)
        await _insert_running_job(
            session,
            worker_name="dead-worker",
            lease_expires_at=expired,
        )

    async with live_claim_db() as session:
        reaped = await reap_expired_jobs(session)

    assert reaped == 1

    async with live_claim_db() as session:
        unchanged = await session.scalar(select(Job).where(Job.id == queued.id))

    assert unchanged is not None
    assert unchanged.status == JobStatus.QUEUED.value


async def test_reap_expired_jobs_respects_limit(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    async with live_claim_db() as session:
        jobs = [
            await _insert_running_job(
                session,
                worker_name="dead-worker",
                lease_expires_at=expired,
            )
            for _ in range(3)
        ]

    async with live_claim_db() as session:
        reaped = await reap_expired_jobs(session, limit=2)

    assert reaped == 2

    async with live_claim_db() as session:
        statuses = {
            job.id: (await session.scalar(select(Job.status).where(Job.id == job.id)))
            for job in jobs
        }

    reaped_statuses = [
        status
        for job_id, status in statuses.items()
        if status in (JobStatus.RETRYING.value, JobStatus.DEAD_LETTER.value)
    ]
    still_running = [
        job_id
        for job_id, status in statuses.items()
        if status == JobStatus.RUNNING.value
    ]
    assert len(reaped_statuses) == 2
    assert len(still_running) == 1


async def test_reap_expired_jobs_returns_zero_when_limit_is_zero(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    async with live_claim_db() as session:
        await _insert_running_job(
            session,
            worker_name="dead-worker",
            lease_expires_at=expired,
        )

    async with live_claim_db() as session:
        reaped = await reap_expired_jobs(session, limit=0)

    assert reaped == 0

    async with live_claim_db() as session:
        still_running = (
            await session.scalars(
                select(Job).where(Job.status == JobStatus.RUNNING.value)
            )
        ).all()

    assert len(still_running) == 1


async def test_reap_expired_jobs_returns_zero_when_none(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_claim_db() as session:
        reaped = await reap_expired_jobs(session)

    assert reaped == 0


async def test_concurrent_reap_expired_jobs_do_not_double_reap(
    live_claim_db: async_sessionmaker[AsyncSession],
) -> None:
    expired = datetime.now(UTC) - timedelta(seconds=1)
    async with live_claim_db() as session:
        for _ in range(4):
            await _insert_running_job(
                session,
                worker_name="dead-worker",
                lease_expires_at=expired,
            )

    async def reap_batch() -> int:
        async with live_claim_db() as session:
            return await reap_expired_jobs(session, limit=4)

    first_count, second_count = await asyncio.gather(reap_batch(), reap_batch())

    assert first_count + second_count == 4
