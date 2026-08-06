from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.config import get_settings
from core.db import Base, dispose_engine, get_engine, get_sessionmaker
from core.models import Job, JobAttempt, JobAttemptStatus, JobStatus
from core.registry import UnknownJobTypeError
from tests.live_postgres import drop_metadata_tables, require_live_postgres_async
from worker.complete import mark_job_failed, mark_job_succeeded
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
    max_retries: int = 3,
    last_error: str | None = None,
    failed_at: datetime | None = None,
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
        max_retries=max_retries,
        last_error=last_error,
        failed_at=failed_at,
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


def _failed_outcome(
    job: Job,
    *,
    error: BaseException | None = None,
    runtime_ms: int = 10,
) -> JobExecutionResult:
    return JobExecutionResult(
        job=job,
        succeeded=False,
        result=None,
        error=ValueError("boom") if error is None else error,
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


async def test_mark_job_failed_handles_unknown_job_type_error(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(
            session,
            worker_name=WORKER_NAME,
            attempts=1,
            max_retries=3,
        )
        original_next_run_at = job.next_run_at

    error = UnknownJobTypeError("missing")
    async with live_complete_db() as session:
        await mark_job_failed(
            session,
            outcome=_failed_outcome(job, error=error),
            worker_name=WORKER_NAME,
        )

    async with live_complete_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))
        attempt = await session.scalar(
            select(JobAttempt).where(JobAttempt.job_id == job.id)
        )

    assert updated is not None
    assert updated.status == JobStatus.RETRYING.value
    assert updated.last_error == "unknown job_type: 'missing'"
    assert updated.failed_at is None
    assert updated.next_run_at > original_next_run_at

    assert attempt is not None
    assert attempt.status == JobAttemptStatus.FAILED.value
    assert attempt.error_message == "unknown job_type: 'missing'"
    assert attempt.attempt_number == job.attempts


async def test_mark_job_failed_retries_when_attempts_below_max(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(
            session,
            worker_name=WORKER_NAME,
            attempts=1,
            max_retries=3,
        )
        original_next_run_at = job.next_run_at

    async with live_complete_db() as session:
        await mark_job_failed(
            session,
            outcome=_failed_outcome(job, error=ValueError("transient")),
            worker_name=WORKER_NAME,
        )

    async with live_complete_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))
        attempt = await session.scalar(
            select(JobAttempt).where(JobAttempt.job_id == job.id)
        )

    assert updated is not None
    assert updated.status == JobStatus.RETRYING.value
    assert updated.attempts == job.attempts
    assert updated.last_error == "transient"
    assert updated.failed_at is None
    assert updated.locked_by is None
    assert updated.locked_at is None
    assert updated.lease_expires_at is None
    assert updated.next_run_at > original_next_run_at

    assert attempt is not None
    assert attempt.status == JobAttemptStatus.FAILED.value
    assert attempt.worker_id == WORKER_NAME
    assert attempt.attempt_number == job.attempts
    assert attempt.error_message == "transient"
    assert attempt.finished_at is not None
    assert attempt.runtime_ms == 10


async def test_mark_job_failed_marks_dead_letter_when_retries_exhausted(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(
            session,
            worker_name=WORKER_NAME,
            attempts=3,
            max_retries=3,
        )

    async with live_complete_db() as session:
        await mark_job_failed(
            session,
            outcome=_failed_outcome(job, error=RuntimeError("permanent")),
            worker_name=WORKER_NAME,
        )

    async with live_complete_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))
        attempt = await session.scalar(
            select(JobAttempt).where(JobAttempt.job_id == job.id)
        )

    assert updated is not None
    assert updated.status == JobStatus.DEAD_LETTER.value
    assert updated.last_error == "permanent"
    assert updated.failed_at is not None
    assert updated.locked_by is None
    assert updated.locked_at is None
    assert updated.lease_expires_at is None

    assert attempt is not None
    assert attempt.status == JobAttemptStatus.FAILED.value
    assert attempt.error_message == "permanent"
    assert attempt.attempt_number == job.attempts


async def test_mark_job_failed_marks_dead_letter_when_attempts_exceed_max_retries(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(
            session,
            worker_name=WORKER_NAME,
            attempts=4,
            max_retries=3,
        )

    async with live_complete_db() as session:
        await mark_job_failed(
            session,
            outcome=_failed_outcome(job, error=RuntimeError("over-retried")),
            worker_name=WORKER_NAME,
        )

    async with live_complete_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))

    assert updated is not None
    assert updated.status == JobStatus.DEAD_LETTER.value
    assert updated.failed_at is not None
    assert updated.last_error == "over-retried"


async def test_mark_job_failed_rejects_succeeded_outcome(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(session, worker_name=WORKER_NAME)

    async with live_complete_db() as session:
        with pytest.raises(ValueError, match="requires a failed outcome"):
            await mark_job_failed(
                session,
                outcome=_succeeded_outcome(job),
                worker_name=WORKER_NAME,
            )


async def test_mark_job_failed_ignores_job_owned_by_other_worker(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(session, worker_name="worker-2")

    async with live_complete_db() as session:
        await mark_job_failed(
            session,
            outcome=_failed_outcome(job),
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
    assert unchanged.last_error is None
    assert attempt is None


async def test_mark_job_failed_with_no_error_leaves_error_fields_none(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(
            session,
            worker_name=WORKER_NAME,
            attempts=1,
            max_retries=3,
        )

    outcome = JobExecutionResult(
        job=job,
        succeeded=False,
        result=None,
        error=None,
        runtime_ms=10,
    )

    async with live_complete_db() as session:
        await mark_job_failed(
            session,
            outcome=outcome,
            worker_name=WORKER_NAME,
        )

    async with live_complete_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))
        attempt = await session.scalar(
            select(JobAttempt).where(JobAttempt.job_id == job.id)
        )

    assert updated is not None
    assert updated.status == JobStatus.RETRYING.value
    assert updated.last_error is None

    assert attempt is not None
    assert attempt.error_message is None


async def test_mark_job_failed_marks_dead_letter_when_max_retries_is_zero(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_complete_db() as session:
        job = await _insert_running_job(
            session,
            worker_name=WORKER_NAME,
            attempts=1,
            max_retries=0,
        )

    async with live_complete_db() as session:
        await mark_job_failed(
            session,
            outcome=_failed_outcome(job, error=ValueError("no retries")),
            worker_name=WORKER_NAME,
        )

    async with live_complete_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))
        attempt = await session.scalar(
            select(JobAttempt).where(JobAttempt.job_id == job.id)
        )

    assert updated is not None
    assert updated.status == JobStatus.DEAD_LETTER.value
    assert updated.failed_at is not None
    assert updated.last_error == "no retries"
    assert updated.locked_by is None
    assert updated.locked_at is None
    assert updated.lease_expires_at is None

    assert attempt is not None
    assert attempt.status == JobAttemptStatus.FAILED.value


async def test_mark_job_failed_clears_stale_failed_at_when_retrying(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    stale_failed_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    async with live_complete_db() as session:
        job = await _insert_running_job(
            session,
            worker_name=WORKER_NAME,
            attempts=1,
            max_retries=3,
            failed_at=stale_failed_at,
        )

    async with live_complete_db() as session:
        await mark_job_failed(
            session,
            outcome=_failed_outcome(job, error=ValueError("transient")),
            worker_name=WORKER_NAME,
        )

    async with live_complete_db() as session:
        updated = await session.scalar(select(Job).where(Job.id == job.id))

    assert updated is not None
    assert updated.status == JobStatus.RETRYING.value
    assert updated.failed_at is None


async def test_mark_job_succeeded_clears_stale_error_fields(
    live_complete_db: async_sessionmaker[AsyncSession],
) -> None:
    stale_failed_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    async with live_complete_db() as session:
        job = await _insert_running_job(
            session,
            worker_name=WORKER_NAME,
            last_error="prior failure",
            failed_at=stale_failed_at,
        )

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
    assert updated.last_error is None
    assert updated.failed_at is None
