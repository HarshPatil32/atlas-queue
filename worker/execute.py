import asyncio
import time
from dataclasses import dataclass
from typing import Any

from core.logging import get_logger
from core.models import Job
from core.registry import UnknownJobTypeError, get_handler

log = get_logger(__name__)


class JobTimeoutError(Exception):
    def __init__(self, timeout_seconds: int) -> None:
        self.timeout_seconds = timeout_seconds
        super().__init__(f"job timed out after {timeout_seconds}s")


@dataclass(frozen=True)
class JobExecutionResult:
    """Outcome of in-process job execution.

    job is the same instance passed to execute_job; callers persisting outcomes
    should read job.id before the loading session closes.
    """

    job: Job
    succeeded: bool
    result: Any | None
    error: BaseException | None
    runtime_ms: int


async def execute_job(job: Job) -> JobExecutionResult:
    start = time.monotonic()
    try:
        handler = get_handler(job.job_type)
        result_value = await asyncio.wait_for(
            handler(job.payload_json),
            timeout=job.timeout_seconds,
        )
    except UnknownJobTypeError as exc:
        runtime_ms = round((time.monotonic() - start) * 1000)
        log.warning(
            "job_execution_unknown_type",
            job_id=job.id,
            job_type=job.job_type,
        )
        return JobExecutionResult(
            job=job,
            succeeded=False,
            result=None,
            error=exc,
            runtime_ms=runtime_ms,
        )
    except TimeoutError:
        runtime_ms = round((time.monotonic() - start) * 1000)
        log.warning(
            "job_execution_timed_out",
            job_id=job.id,
            job_type=job.job_type,
            timeout_seconds=job.timeout_seconds,
        )
        return JobExecutionResult(
            job=job,
            succeeded=False,
            result=None,
            error=JobTimeoutError(job.timeout_seconds),
            runtime_ms=runtime_ms,
        )
    except Exception as exc:
        runtime_ms = round((time.monotonic() - start) * 1000)
        log.exception(
            "job_execution_failed",
            job_id=job.id,
            job_type=job.job_type,
        )
        return JobExecutionResult(
            job=job,
            succeeded=False,
            result=None,
            error=exc,
            runtime_ms=runtime_ms,
        )
    runtime_ms = round((time.monotonic() - start) * 1000)
    return JobExecutionResult(
        job=job,
        succeeded=True,
        result=result_value,
        error=None,
        runtime_ms=runtime_ms,
    )
