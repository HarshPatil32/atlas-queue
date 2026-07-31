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
from worker.claim import (
    claim_jobs,
    count_in_flight_jobs,
    extend_lease,
    reap_expired_jobs,
    release_in_flight_jobs,
)
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


async def _heartbeat_job_lease(
    *,
    job_id: int,
    worker_name: str,
    lease_seconds: int,
    interval_seconds: float,
    shutdown_event: asyncio.Event,
) -> None:
    while not shutdown_event.is_set():
        try:
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(),
                    timeout=interval_seconds,
                )
                return
            except TimeoutError:
                pass

            async with get_session() as session:
                extended = await extend_lease(
                    session,
                    job_id=job_id,
                    worker_name=worker_name,
                    lease_seconds=lease_seconds,
                )
            if not extended:
                log.debug("job_lease_heartbeat_stopped", job_id=job_id)
                return
        except Exception:
            log.exception("job_lease_heartbeat_failed", job_id=job_id)
            return


def _prune_done_heartbeat_tasks(
    heartbeat_tasks: dict[int, asyncio.Task[None]],
) -> None:
    done_job_ids = [job_id for job_id, task in heartbeat_tasks.items() if task.done()]
    for job_id in done_job_ids:
        task = heartbeat_tasks[job_id]
        if (exc := task.exception()) is not None:
            log.error(
                "job_lease_heartbeat_task_failed",
                job_id=job_id,
                exc_info=exc,
            )
        del heartbeat_tasks[job_id]


async def _cancel_heartbeat_tasks(
    heartbeat_tasks: dict[int, asyncio.Task[None]],
) -> None:
    for task in heartbeat_tasks.values():
        if not task.done():
            task.cancel()
    for job_id, task in heartbeat_tasks.items():
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("job_lease_heartbeat_task_failed", job_id=job_id)


async def run_loop(
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
    current_interval = poll_interval_seconds
    last_reap_at = 0.0
    loop_clock = asyncio.get_running_loop()
    while not shutdown_event.is_set():
        async with get_session() as session:
            await heartbeat(session, name=name)

        if loop_clock.time() - last_reap_at >= reaper_interval_seconds:
            async with get_session() as session:
                reaped = await reap_expired_jobs(session)
            if reaped:
                log.info("reaper_requeued_jobs", count=reaped)
            last_reap_at = loop_clock.time()

        async with get_session() as session:
            in_flight = await count_in_flight_jobs(session, worker_name=name)
            remaining = max(0, limit - in_flight)
            claimed = await claim_jobs(
                session,
                queues=queues,
                worker_name=name,
                limit=remaining,
                lease_seconds=lease_seconds,
            )
        if claimed:
            log.debug("poll_claimed", count=len(claimed))
            current_interval = poll_interval_seconds
            for job in claimed:
                existing = heartbeat_tasks.get(job.id)
                if existing is not None and not existing.done():
                    continue
                heartbeat_tasks[job.id] = asyncio.create_task(
                    _heartbeat_job_lease(
                        job_id=job.id,
                        worker_name=name,
                        lease_seconds=lease_seconds,
                        interval_seconds=lease_heartbeat_interval_seconds,
                        shutdown_event=shutdown_event,
                    )
                )
        else:
            current_interval = min(
                current_interval * backoff_multiplier,
                backoff_max_seconds,
            )
            log.debug("poll_backoff", interval_seconds=current_interval)

        _prune_done_heartbeat_tasks(heartbeat_tasks)

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

    heartbeat_tasks: dict[int, asyncio.Task[None]] = {}
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
            lease_heartbeat_interval_seconds=settings.lease_heartbeat_interval_seconds,
            poll_interval_seconds=settings.poll_interval_seconds,
            backoff_multiplier=settings.poll_backoff_multiplier,
            backoff_max_seconds=settings.poll_backoff_max_seconds,
            reaper_interval_seconds=settings.reaper_interval_seconds,
            shutdown_event=shutdown_event,
            heartbeat_tasks=heartbeat_tasks,
        )
    finally:
        try:
            await _cancel_heartbeat_tasks(heartbeat_tasks)
        except Exception:
            log.exception("worker_cancel_heartbeat_tasks_failed")
        try:
            async with get_session() as session:
                released = await release_in_flight_jobs(
                    session, worker_name=worker_name
                )
            if released:
                log.info("worker_released_jobs", count=released)
        except Exception:
            log.exception("worker_release_jobs_failed")
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
