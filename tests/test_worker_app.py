import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import TypedDict, Unpack

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from core.config import get_settings
from core.db import Base, dispose_engine, get_engine, get_session, get_sessionmaker
from core.models import Job, JobStatus, Worker, WorkerStatus
from tests.live_postgres import drop_metadata_tables, require_live_postgres_async
from worker.app import (
    _cancel_heartbeat_tasks,
    get_worker,
    heartbeat,
    main,
    mark_offline,
    register_worker,
    run_loop,
)
from worker.claim import claim_jobs, count_in_flight_jobs
from worker.cli import WorkerArgs

DEFAULT_LEASE_SECONDS = 30


class RunLoopKwargs(TypedDict):
    queues: list[str]
    limit: int
    lease_seconds: int
    lease_heartbeat_interval_seconds: float
    poll_interval_seconds: float
    backoff_multiplier: float
    backoff_max_seconds: float
    reaper_interval_seconds: float


DEFAULT_RUN_LOOP_KWARGS: RunLoopKwargs = {
    "queues": ["default"],
    "limit": 1,
    "lease_seconds": DEFAULT_LEASE_SECONDS,
    "lease_heartbeat_interval_seconds": 0.05,
    "poll_interval_seconds": 0.05,
    "backoff_multiplier": 2.0,
    "backoff_max_seconds": 1.0,
    "reaper_interval_seconds": 0.05,
}


async def _cancel_background_task(task: asyncio.Task[object]) -> None:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _run_loop(
    *,
    name: str,
    shutdown_event: asyncio.Event,
    **kwargs: Unpack[RunLoopKwargs],
) -> None:
    heartbeat_tasks: dict[int, asyncio.Task[None]] = {}
    try:
        await run_loop(
            name=name,
            shutdown_event=shutdown_event,
            heartbeat_tasks=heartbeat_tasks,
            **kwargs,
        )
    finally:
        await _cancel_heartbeat_tasks(heartbeat_tasks)


@pytest.fixture
async def live_worker_db(
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
    job_type: str = "test",
) -> Job:
    now = datetime.now(UTC)
    job = Job(
        queue=queue,
        job_type=job_type,
        payload_json={},
        status=status,
        priority=0,
        next_run_at=now,
        created_at=now,
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
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def test_register_worker_inserts_row(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_worker_db() as session:
        await register_worker(
            session,
            name="worker-1",
            hostname="host-a",
            queues=["default", "priority"],
        )

    async with live_worker_db() as session:
        worker = await session.scalar(
            select(Worker).where(Worker.worker_name == "worker-1")
        )

    assert worker is not None
    assert worker.hostname == "host-a"
    assert worker.queues == ["default", "priority"]
    assert worker.status == WorkerStatus.RUNNING.value
    assert worker.last_heartbeat_at is not None


async def test_register_worker_upserts_existing_name(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_worker_db() as session:
        await register_worker(
            session,
            name="worker-1",
            hostname="host-a",
            queues=["default"],
        )

    async with live_worker_db() as session:
        first = await get_worker(session, name="worker-1")
        assert first is not None
        first_started_at = first.started_at

    async with live_worker_db() as session:
        await register_worker(
            session,
            name="worker-1",
            hostname="host-b",
            queues=["other"],
        )

    async with live_worker_db() as session:
        workers = (
            await session.scalars(
                select(Worker).where(Worker.worker_name == "worker-1")
            )
        ).all()
        worker = workers[0]

    assert len(workers) == 1
    assert worker.hostname == "host-b"
    assert worker.queues == ["other"]
    assert worker.status == WorkerStatus.RUNNING.value
    assert worker.started_at == first_started_at


async def test_heartbeat_updates_last_heartbeat_at(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_worker_db() as session:
        await register_worker(
            session,
            name="worker-1",
            hostname="host-a",
            queues=["default"],
        )

    async with live_worker_db() as session:
        before = await get_worker(session, name="worker-1")
        assert before is not None
        first_heartbeat = before.last_heartbeat_at
        assert first_heartbeat is not None

    await asyncio.sleep(0.01)

    async with live_worker_db() as session:
        await heartbeat(session, name="worker-1")

    async with live_worker_db() as session:
        after = await get_worker(session, name="worker-1")
        assert after is not None
        assert after.last_heartbeat_at is not None
        assert after.last_heartbeat_at > first_heartbeat


async def test_run_loop_updates_heartbeat_until_shutdown(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_worker_db() as session:
        await register_worker(
            session,
            name="worker-loop",
            hostname="host-a",
            queues=["default"],
        )
        initial = await get_worker(session, name="worker-loop")
        assert initial is not None
        initial_heartbeat = initial.last_heartbeat_at
        assert initial_heartbeat is not None

    shutdown_event = asyncio.Event()

    async def stop_after_loop_heartbeat() -> None:
        for _ in range(20):
            async with get_session() as session:
                worker = await get_worker(session, name="worker-loop")
                if (
                    worker is not None
                    and worker.last_heartbeat_at is not None
                    and worker.last_heartbeat_at > initial_heartbeat
                ):
                    shutdown_event.set()
                    return
            await asyncio.sleep(0.05)

    stopper = asyncio.create_task(stop_after_loop_heartbeat())
    try:
        await asyncio.wait_for(
            _run_loop(
                name="worker-loop",
                shutdown_event=shutdown_event,
                **DEFAULT_RUN_LOOP_KWARGS,
            ),
            timeout=2.0,
        )
    finally:
        await _cancel_background_task(stopper)

    async with live_worker_db() as session:
        worker = await get_worker(session, name="worker-loop")
        assert worker is not None
        assert worker.last_heartbeat_at is not None
        assert worker.last_heartbeat_at > initial_heartbeat


async def test_run_loop_claims_available_job(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_worker_db() as session:
        await register_worker(
            session,
            name="worker-loop",
            hostname="host-a",
            queues=["default"],
        )
        job = await _insert_job(session)

    shutdown_event = asyncio.Event()

    async def stop_after_claim() -> None:
        for _ in range(50):
            async with get_session() as session:
                claimed_job = await session.scalar(select(Job).where(Job.id == job.id))
                if (
                    claimed_job is not None
                    and claimed_job.status == JobStatus.RUNNING.value
                ):
                    shutdown_event.set()
                    return
            await asyncio.sleep(0.02)

    stopper = asyncio.create_task(stop_after_claim())
    try:
        await asyncio.wait_for(
            _run_loop(
                name="worker-loop",
                shutdown_event=shutdown_event,
                **DEFAULT_RUN_LOOP_KWARGS,
            ),
            timeout=3.0,
        )
    finally:
        await _cancel_background_task(stopper)

    async with live_worker_db() as session:
        claimed_job = await session.scalar(select(Job).where(Job.id == job.id))

    assert claimed_job is not None
    assert claimed_job.status == JobStatus.RUNNING.value
    assert claimed_job.locked_by == "worker-loop"


async def test_main_shutdown_releases_claimed_jobs_and_marks_offline(
    live_worker_db: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_name = "worker-main-shutdown"

    async with live_worker_db() as session:
        job = await _insert_job(session)

    async def claim_then_shutdown(
        *,
        name: str,
        queues: list[str],
        limit: int,
        lease_seconds: int,
        lease_heartbeat_interval_seconds: float,
        poll_interval_seconds: float,
        backoff_multiplier: float,
        backoff_max_seconds: float,
        reaper_interval_seconds: float,
        shutdown_event: asyncio.Event,
        heartbeat_tasks: dict[int, asyncio.Task[None]],
    ) -> None:
        async with get_session() as session:
            await claim_jobs(
                session,
                queues=queues,
                worker_name=name,
                limit=limit,
                lease_seconds=lease_seconds,
            )
        shutdown_event.set()

    monkeypatch.setattr("worker.app.run_loop", claim_then_shutdown)

    await main(WorkerArgs(queues=["default"], concurrency=1, name=worker_name))

    async with live_worker_db() as session:
        released_job = await session.scalar(select(Job).where(Job.id == job.id))
        worker = await get_worker(session, name=worker_name)

    assert released_job is not None
    assert released_job.status == JobStatus.QUEUED.value
    assert released_job.locked_by is None
    assert released_job.locked_at is None
    assert released_job.lease_expires_at is None
    assert worker is not None
    assert worker.status == WorkerStatus.OFFLINE.value


async def test_run_loop_backs_off_when_queue_empty(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_worker_db() as session:
        await register_worker(
            session,
            name="worker-loop",
            hostname="host-a",
            queues=["default"],
        )

    shutdown_event = asyncio.Event()
    heartbeat_times: list[datetime] = []

    async def record_heartbeats() -> None:
        last: datetime | None = None
        for _ in range(100):
            async with get_session() as session:
                worker = await get_worker(session, name="worker-loop")
                if worker is not None and worker.last_heartbeat_at is not None:
                    current = worker.last_heartbeat_at
                    if last != current:
                        heartbeat_times.append(current)
                        last = current
                        if len(heartbeat_times) >= 4:
                            shutdown_event.set()
                            return
            await asyncio.sleep(0.01)

    stopper = asyncio.create_task(record_heartbeats())
    try:
        await asyncio.wait_for(
            _run_loop(
                name="worker-loop",
                shutdown_event=shutdown_event,
                **DEFAULT_RUN_LOOP_KWARGS,
            ),
            timeout=5.0,
        )
    finally:
        await _cancel_background_task(stopper)

    gaps = [
        (heartbeat_times[index + 1] - heartbeat_times[index]).total_seconds()
        for index in range(len(heartbeat_times) - 1)
    ]
    assert len(gaps) >= 2
    assert gaps[1] > gaps[0] * 1.5


async def test_run_loop_resets_backoff_after_claim(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    poll_interval = 0.05
    backoff_max = 0.2

    async with live_worker_db() as session:
        await register_worker(
            session,
            name="worker-loop",
            hostname="host-a",
            queues=["default"],
        )

    shutdown_event = asyncio.Event()
    heartbeat_times: list[datetime] = []
    job_id: int | None = None

    async def watch_loop() -> None:
        nonlocal job_id
        last: datetime | None = None
        for _ in range(200):
            async with get_session() as session:
                worker = await get_worker(session, name="worker-loop")
                if worker is not None and worker.last_heartbeat_at is not None:
                    current = worker.last_heartbeat_at
                    if last != current:
                        heartbeat_times.append(current)
                        last = current

                if job_id is None and len(heartbeat_times) >= 3:
                    job = await _insert_job(session)
                    job_id = job.id

                if job_id is not None:
                    claimed_job = await session.scalar(
                        select(Job).where(Job.id == job_id)
                    )
                    if (
                        claimed_job is not None
                        and claimed_job.status == JobStatus.RUNNING.value
                        and len(heartbeat_times) >= 5
                    ):
                        shutdown_event.set()
                        return
            await asyncio.sleep(0.01)

    stopper = asyncio.create_task(watch_loop())
    try:
        await asyncio.wait_for(
            _run_loop(
                name="worker-loop",
                shutdown_event=shutdown_event,
                queues=["default"],
                limit=1,
                lease_seconds=DEFAULT_LEASE_SECONDS,
                lease_heartbeat_interval_seconds=0.05,
                poll_interval_seconds=poll_interval,
                backoff_multiplier=2.0,
                backoff_max_seconds=backoff_max,
                reaper_interval_seconds=0.05,
            ),
            timeout=5.0,
        )
    finally:
        await _cancel_background_task(stopper)

    pre_claim_gaps = [
        (heartbeat_times[i + 1] - heartbeat_times[i]).total_seconds() for i in range(2)
    ]
    post_claim_gaps = [
        (heartbeat_times[i + 1] - heartbeat_times[i]).total_seconds()
        for i in range(3, len(heartbeat_times) - 1)
    ]

    assert job_id is not None
    assert len(heartbeat_times) >= 5
    assert len(pre_claim_gaps) == 2
    assert pre_claim_gaps[1] > pre_claim_gaps[0] * 1.5
    assert len(post_claim_gaps) >= 1
    assert min(post_claim_gaps) < backoff_max * 0.75


async def test_run_loop_respects_concurrency_limit(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    worker_name = "worker-loop"
    concurrency = 2

    async with live_worker_db() as session:
        await register_worker(
            session,
            name=worker_name,
            hostname="host-a",
            queues=["default"],
        )
        for _ in range(5):
            await _insert_job(session)

    shutdown_event = asyncio.Event()
    max_in_flight = 0

    async def watch_in_flight() -> None:
        nonlocal max_in_flight
        for _ in range(100):
            async with get_session() as session:
                in_flight = await count_in_flight_jobs(session, worker_name=worker_name)
                max_in_flight = max(max_in_flight, in_flight)
                if in_flight >= concurrency:
                    shutdown_event.set()
                    return
            await asyncio.sleep(0.02)

    stopper = asyncio.create_task(watch_in_flight())
    try:
        await asyncio.wait_for(
            _run_loop(
                name=worker_name,
                shutdown_event=shutdown_event,
                queues=["default"],
                limit=concurrency,
                lease_seconds=DEFAULT_LEASE_SECONDS,
                lease_heartbeat_interval_seconds=0.05,
                poll_interval_seconds=0.05,
                backoff_multiplier=2.0,
                backoff_max_seconds=1.0,
                reaper_interval_seconds=0.05,
            ),
            timeout=5.0,
        )
    finally:
        await _cancel_background_task(stopper)

    async with live_worker_db() as session:
        in_flight = await count_in_flight_jobs(session, worker_name=worker_name)
        queued = (
            await session.scalars(
                select(Job).where(Job.status == JobStatus.QUEUED.value)
            )
        ).all()

    assert shutdown_event.is_set()
    assert max_in_flight <= concurrency
    assert in_flight == concurrency
    assert len(queued) == 5 - concurrency


async def test_run_loop_claims_only_remaining_capacity(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    worker_name = "worker-loop"

    async with live_worker_db() as session:
        await register_worker(
            session,
            name=worker_name,
            hostname="host-a",
            queues=["default"],
        )
        await _insert_running_job(session, worker_name=worker_name)
        for _ in range(3):
            await _insert_job(session)

    shutdown_event = asyncio.Event()

    async def stop_at_capacity() -> None:
        for _ in range(100):
            async with get_session() as session:
                in_flight = await count_in_flight_jobs(session, worker_name=worker_name)
                if in_flight >= 2:
                    shutdown_event.set()
                    return
            await asyncio.sleep(0.02)

    stopper = asyncio.create_task(stop_at_capacity())
    try:
        await asyncio.wait_for(
            _run_loop(
                name=worker_name,
                shutdown_event=shutdown_event,
                queues=["default"],
                limit=2,
                lease_seconds=DEFAULT_LEASE_SECONDS,
                lease_heartbeat_interval_seconds=0.05,
                poll_interval_seconds=0.05,
                backoff_multiplier=2.0,
                backoff_max_seconds=1.0,
                reaper_interval_seconds=0.05,
            ),
            timeout=5.0,
        )
    finally:
        await _cancel_background_task(stopper)

    async with live_worker_db() as session:
        in_flight = await count_in_flight_jobs(session, worker_name=worker_name)
        queued = (
            await session.scalars(
                select(Job).where(Job.status == JobStatus.QUEUED.value)
            )
        ).all()

    assert in_flight == 2
    assert len(queued) == 2


async def test_run_loop_ignores_expired_in_flight_for_capacity(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    worker_name = "worker-loop"
    expired = datetime.now(UTC) - timedelta(seconds=1)

    async with live_worker_db() as session:
        await register_worker(
            session,
            name=worker_name,
            hostname="host-a",
            queues=["default"],
        )
        await _insert_running_job(
            session,
            worker_name=worker_name,
            lease_expires_at=expired,
        )
        queued_job = await _insert_job(session)

    shutdown_event = asyncio.Event()

    async def stop_after_new_claim() -> None:
        for _ in range(100):
            async with get_session() as session:
                claimed_job = await session.scalar(
                    select(Job).where(Job.id == queued_job.id)
                )
                if (
                    claimed_job is not None
                    and claimed_job.status == JobStatus.RUNNING.value
                ):
                    shutdown_event.set()
                    return
            await asyncio.sleep(0.02)

    stopper = asyncio.create_task(stop_after_new_claim())
    try:
        await asyncio.wait_for(
            _run_loop(
                name=worker_name,
                shutdown_event=shutdown_event,
                queues=["default"],
                limit=1,
                lease_seconds=DEFAULT_LEASE_SECONDS,
                lease_heartbeat_interval_seconds=0.05,
                poll_interval_seconds=0.05,
                backoff_multiplier=2.0,
                backoff_max_seconds=1.0,
                reaper_interval_seconds=0.05,
            ),
            timeout=5.0,
        )
    finally:
        await _cancel_background_task(stopper)

    async with live_worker_db() as session:
        claimed_job = await session.scalar(select(Job).where(Job.id == queued_job.id))

    assert claimed_job is not None
    assert claimed_job.status == JobStatus.RUNNING.value
    assert claimed_job.locked_by == worker_name


async def test_run_loop_extends_job_lease_while_in_flight(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    worker_name = "worker-loop"
    lease_seconds = 1
    heartbeat_interval = 0.2

    async with live_worker_db() as session:
        await register_worker(
            session,
            name=worker_name,
            hostname="host-a",
            queues=["default"],
        )
        job = await _insert_job(session)

    shutdown_event = asyncio.Event()
    original_expiry: datetime | None = None

    async def wait_for_lease_extension() -> None:
        nonlocal original_expiry
        for _ in range(100):
            async with get_session() as session:
                claimed_job = await session.scalar(select(Job).where(Job.id == job.id))
                if claimed_job is None:
                    await asyncio.sleep(0.02)
                    continue

                if (
                    claimed_job.status == JobStatus.RUNNING.value
                    and claimed_job.locked_by == worker_name
                ):
                    if original_expiry is None:
                        original_expiry = claimed_job.lease_expires_at
                    elif (
                        claimed_job.lease_expires_at is not None
                        and original_expiry is not None
                        and claimed_job.lease_expires_at > original_expiry
                    ):
                        shutdown_event.set()
                        return
            await asyncio.sleep(0.02)

    stopper = asyncio.create_task(wait_for_lease_extension())
    try:
        await asyncio.wait_for(
            _run_loop(
                name=worker_name,
                shutdown_event=shutdown_event,
                queues=["default"],
                limit=1,
                lease_seconds=lease_seconds,
                lease_heartbeat_interval_seconds=heartbeat_interval,
                poll_interval_seconds=0.05,
                backoff_multiplier=2.0,
                backoff_max_seconds=1.0,
                reaper_interval_seconds=0.05,
            ),
            timeout=5.0,
        )
    finally:
        await _cancel_background_task(stopper)

    assert original_expiry is not None
    assert shutdown_event.is_set()


async def test_run_loop_logs_when_job_lease_heartbeat_fails(
    live_worker_db: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_name = "worker-loop"

    async with live_worker_db() as session:
        await register_worker(
            session,
            name=worker_name,
            hostname="host-a",
            queues=["default"],
        )
        job = await _insert_job(session)

    shutdown_event = asyncio.Event()
    heartbeat_failed = asyncio.Event()

    async def failing_extend_lease(*args: object, **kwargs: object) -> bool:
        heartbeat_failed.set()
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("worker.app.extend_lease", failing_extend_lease)

    async def stop_after_heartbeat_failure() -> None:
        await heartbeat_failed.wait()
        shutdown_event.set()

    stopper = asyncio.create_task(stop_after_heartbeat_failure())
    try:
        with capture_logs() as cap_logs:
            await asyncio.wait_for(
                _run_loop(
                    name=worker_name,
                    shutdown_event=shutdown_event,
                    queues=["default"],
                    limit=1,
                    lease_seconds=DEFAULT_LEASE_SECONDS,
                    lease_heartbeat_interval_seconds=0.05,
                    poll_interval_seconds=0.05,
                    backoff_multiplier=2.0,
                    backoff_max_seconds=1.0,
                    reaper_interval_seconds=0.05,
                ),
                timeout=5.0,
            )
    finally:
        await _cancel_background_task(stopper)

    assert any(
        entry.get("event") == "job_lease_heartbeat_failed"
        and entry.get("job_id") == job.id
        for entry in cap_logs
    )


async def test_run_loop_reaps_expired_lease_from_dead_worker(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    worker_name = "worker-loop"
    expired = datetime.now(UTC) - timedelta(seconds=1)

    async with live_worker_db() as session:
        await register_worker(
            session,
            name=worker_name,
            hostname="host-a",
            queues=["default"],
        )
        job = await _insert_running_job(
            session,
            worker_name="dead-worker",
            lease_expires_at=expired,
            attempts=1,
            max_retries=3,
        )

    shutdown_event = asyncio.Event()

    async def stop_after_reap() -> None:
        for _ in range(100):
            async with get_session() as session:
                reaped_job = await session.scalar(select(Job).where(Job.id == job.id))
                if (
                    reaped_job is not None
                    and reaped_job.status == JobStatus.RETRYING.value
                    and reaped_job.locked_by is None
                ):
                    shutdown_event.set()
                    return
            await asyncio.sleep(0.02)

    stopper = asyncio.create_task(stop_after_reap())
    try:
        with capture_logs() as cap_logs:
            await asyncio.wait_for(
                _run_loop(
                    name=worker_name,
                    shutdown_event=shutdown_event,
                    **DEFAULT_RUN_LOOP_KWARGS,
                ),
                timeout=5.0,
            )
    finally:
        await _cancel_background_task(stopper)

    async with live_worker_db() as session:
        reaped_job = await session.scalar(select(Job).where(Job.id == job.id))

    assert reaped_job is not None
    assert reaped_job.status == JobStatus.RETRYING.value
    assert reaped_job.locked_by is None
    assert any(entry.get("event") == "reaper_requeued_jobs" for entry in cap_logs)


async def test_mark_offline_sets_status(
    live_worker_db: async_sessionmaker[AsyncSession],
) -> None:
    async with live_worker_db() as session:
        await register_worker(
            session,
            name="worker-1",
            hostname="host-a",
            queues=["default"],
        )

    async with live_worker_db() as session:
        await mark_offline(session, name="worker-1")

    async with live_worker_db() as session:
        worker = await get_worker(session, name="worker-1")
        assert worker is not None
        assert worker.status == WorkerStatus.OFFLINE.value
