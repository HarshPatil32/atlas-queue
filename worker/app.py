import asyncio
import signal
import socket
from typing import cast

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import get_settings
from core.db import dispose_engine, get_session
from core.logging import (
    bind_worker_context,
    clear_context,
    configure_logging,
    get_logger,
)
from core.models import Worker, WorkerStatus
from worker.claim import claim_jobs
from worker.cli import WorkerArgs, default_worker_name

log = get_logger(__name__)


async def register_worker(
    session: AsyncSession,
    *,
    name: str,
    hostname: str,
    queues: list[str],
) -> None:
    stmt = (
        insert(Worker)
        .values(
            worker_name=name,
            hostname=hostname,
            queues=queues,
            status=WorkerStatus.RUNNING.value,
            last_heartbeat_at=func.now(),
        )
        .on_conflict_do_update(
            index_elements=[Worker.worker_name],
            set_={
                "hostname": hostname,
                "queues": queues,
                "status": WorkerStatus.RUNNING.value,
                "last_heartbeat_at": func.now(),
                # started_at is intentionally omitted so re-registration keeps it.
            },
        )
    )
    await session.execute(stmt)
    await session.commit()


async def heartbeat(session: AsyncSession, *, name: str) -> None:
    await session.execute(
        update(Worker)
        .where(Worker.worker_name == name)
        .values(last_heartbeat_at=func.now())
    )
    await session.commit()


async def mark_offline(session: AsyncSession, *, name: str) -> None:
    await session.execute(
        update(Worker)
        .where(Worker.worker_name == name)
        .values(status=WorkerStatus.OFFLINE.value)
    )
    await session.commit()


async def run_loop(
    *,
    name: str,
    queues: list[str],
    limit: int,
    lease_seconds: int,
    poll_interval_seconds: float,
    backoff_multiplier: float,
    backoff_max_seconds: float,
    shutdown_event: asyncio.Event,
) -> None:
    current_interval = poll_interval_seconds
    while not shutdown_event.is_set():
        async with get_session() as session:
            await heartbeat(session, name=name)
        async with get_session() as session:
            claimed = await claim_jobs(
                session,
                queues=queues,
                worker_name=name,
                limit=limit,
                lease_seconds=lease_seconds,
            )
        if claimed:
            log.debug("poll_claimed", count=len(claimed))
            current_interval = poll_interval_seconds
        else:
            current_interval = min(
                current_interval * backoff_multiplier,
                backoff_max_seconds,
            )
            log.debug("poll_backoff", interval_seconds=current_interval)
        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=current_interval,
            )
        except TimeoutError:
            continue


async def main(args: WorkerArgs) -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)

    worker_name = args.name or default_worker_name()
    bind_worker_context(worker_id=worker_name)

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    hostname = socket.gethostname()
    log.info(
        "worker_starting",
        worker_name=worker_name,
        queues=args.queues,
        concurrency=args.concurrency,
    )

    try:
        async with get_session() as session:
            await register_worker(
                session,
                name=worker_name,
                hostname=hostname,
                queues=args.queues,
            )

        await run_loop(
            name=worker_name,
            queues=args.queues,
            limit=args.concurrency,
            lease_seconds=settings.lease_ttl_seconds,
            poll_interval_seconds=settings.poll_interval_seconds,
            backoff_multiplier=settings.poll_backoff_multiplier,
            backoff_max_seconds=settings.poll_backoff_max_seconds,
            shutdown_event=shutdown_event,
        )
    finally:
        try:
            async with get_session() as session:
                await mark_offline(session, name=worker_name)
        except Exception:
            log.exception("worker_shutdown_failed")
        await dispose_engine()
        clear_context()
        log.info("worker_stopped", worker_name=worker_name)


async def get_worker(
    session: AsyncSession,
    *,
    name: str,
) -> Worker | None:
    return cast(
        Worker | None,
        await session.scalar(select(Worker).where(Worker.worker_name == name)),
    )
