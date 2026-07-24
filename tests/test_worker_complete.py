from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.config import get_settings
from core.db import Base, dispose_engine, get_engine, get_sessionmaker
from core.models import Job, JobAttempt, JobAttemptStatus, JobStatus
from tests.live_postgres import drop_metadata_tables, require_live_postgres_async
from worker.complete import mark_job_succeeded
from worker.execute import JobExecutionResult

DEFAULT_LEASE_SECONDS = 30
WORKER_NAME = "worker-1"


@pytest.fixture
async def live_complete_db(
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


async def _insert_running_job(
    session: AsyncSession,
    *,
    worker_name: str,
    queue: str = "default",
    lease_expires_at: datetime | None = None,
    attempts: int = 1,
) -> Job:
    now = datetime.now(UTC)
    job = Job(
        queue=queue,
        job_type="test",
        payload_json={},
        status=JobStatus.RUNNING.value,
        priority=0,
        next_run_at=now,
        created_at=now,
        locked_by=worker_name,
        locked_at=now,
        lease_expires_at=(
            lease_expires_at or now + timedelta(seconds=DEFAULT_LEASE_SECONDS)
        ),
        attempts=attempts,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


def _succeeded_outcome(
    job: Job,
    *,
    runtime_ms: int = 42,
    result: dict | None = None,
) -> JobExecutionResult:
    return JobExecutionResult(
        job=job,
        succeeded=True,
        result={"ok": True} if result is None else result,
        error=None,
        runtime_ms=runtime_ms,
    )


async def test_mark_job_succeeded_updates_job_status(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(session, worker_name=WORKER_NAME)

    async with live_complete_db() as session:
        await mark_job_succeeded(
            session,
            outcome=_succeeded_outcome(job),
            worker_name=WORKER_NAME,
        )

    async with live_complete_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))

    assert updated is not None
    assert updated.status == JobStatus.SUCCEEDED.value
    assert updated.completed_at is not None
    assert updated.locked_by is None
    assert updated.locked_at is None
    assert updated.lease_expires_at is None


async def test_mark_job_succeeded_inserts_job_attempts_row(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(session, worker_name=WORKER_NAME)

    async with live_complete_db() as session:
        await mark_job_succeeded(
            session,
            outcome=_succeeded_outcome(job, runtime_ms=42),
            worker_name=WORKER_NAME,
        )

    async with live_complete_db() as session:
        attempt = await session.scalar(
            select(JobAttempt).where(JobAttempt.job_id == job.id)
        )

    assert attempt is not None
    assert attempt.status == JobAttemptStatus.SUCCEEDED.value
    assert attempt.worker_id == WORKER_NAME
    assert attempt.attempt_number == job.attempts
    assert attempt.finished_at is not None
    assert attempt.runtime_ms == 42


async def test_mark_job_succeeded_rejects_failed_outcome(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(session, worker_name=WORKER_NAME)

    outcome = JobExecutionResult(
        job=job,
        succeeded=False,
        result=None,
        error=ValueError("boom"),
        runtime_ms=10,
    )

    async with live_complete_db() as session:
        with pytest.raises(ValueError, match="requires a succeeded outcome"):
            await mark_job_succeeded(
                session,
                outcome=outcome,
                worker_name=WORKER_NAME,
            )


async def test_mark_job_succeeded_ignores_job_owned_by_other_worker(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(session, worker_name="worker-2")

    async with live_complete_db() as session:
        await mark_job_succeeded(
            session,
            outcome=_succeeded_outcome(job),
            worker_name=WORKER_NAME,
        )

    async with live_complete_db() as session:
        unchanged = await session.scalar(select(Job).where(Job.id == job.id))
        attempt = await session.scalar(
            select(JobAttempt).where(JobAttempt.job_id == job.id)
        )

    assert unchanged is not None
    assert unchanged.status == JobStatus.RUNNING.value
    assert unchanged.locked_by == "worker-2"
    assert attempt is None
