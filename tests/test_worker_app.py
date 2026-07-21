import asyncio
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from core.config import get_settings
from core.db import Base, dispose_engine, get_engine, get_session, get_sessionmaker
from core.models import Worker, WorkerStatus
from tests.live_postgres import drop_metadata_tables, require_live_postgres_async
from worker.app import get_worker, heartbeat, mark_offline, register_worker, run_loop


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
            run_loop(
                name="worker-loop",
                poll_interval_seconds=0.05,
                shutdown_event=shutdown_event,
            ),
            timeout=2.0,
        )
    finally:
        stopper.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopper

    async with live_worker_db() as session:
        worker = await get_worker(session, name="worker-loop")
        assert worker is not None
        assert worker.last_heartbeat_at is not None
        assert worker.last_heartbeat_at > initial_heartbeat


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
