import time
from dataclasses import dataclass
from typing import Any

from core.logging import get_logger
from core.models import Job
from core.registry import UnknownJobTypeError, get_handler

log = get_logger(__name__)


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
        result_value = await handler(job.payload_json)
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
