import asyncio
from collections.abc import Iterator

import pytest

from core.models import Job
from core.registry import UnknownJobTypeError, clear_registry, register
from worker.execute import JobExecutionResult, JobTimeoutError, execute_job


@pytest.fixture(autouse=True)
def _clean_registry() -> Iterator[None]:
    clear_registry()
    yield
    clear_registry()


def _job(
    *,
    job_type: str = "echo",
    payload: dict | None = None,
    timeout_seconds: int | None = None,
) -> Job:
    # timeout_seconds=None disables enforcement (asyncio.wait_for no-op).
    # timeout_seconds=0 is an immediate timeout, not "no timeout".
    return Job(
        queue="default",
        job_type=job_type,
        payload_json=payload or {},
        timeout_seconds=timeout_seconds,
    )


async def test_execute_job_returns_handler_result() -> None:
    @register("echo")
    async def echo(payload: dict) -> dict:
        return payload

    outcome = await execute_job(_job(payload={"value": "ok"}))

    assert isinstance(outcome, JobExecutionResult)
    assert outcome.succeeded is True
    assert outcome.result == {"value": "ok"}
    assert outcome.error is None
    assert isinstance(outcome.runtime_ms, int)
    assert outcome.runtime_ms >= 0


async def test_execute_job_captures_handler_exception() -> None:
    @register("fail")
    async def fail(_payload: dict) -> None:
        raise ValueError("boom")

    outcome = await execute_job(_job(job_type="fail"))

    assert outcome.succeeded is False
    assert outcome.result is None
    assert isinstance(outcome.error, ValueError)
    assert str(outcome.error) == "boom"
    assert outcome.runtime_ms >= 0


async def test_execute_job_captures_unknown_job_type() -> None:
    outcome = await execute_job(_job(job_type="missing"))

    assert outcome.succeeded is False
    assert outcome.result is None
    assert isinstance(outcome.error, UnknownJobTypeError)
    assert outcome.error.job_type == "missing"
    assert outcome.runtime_ms >= 0


async def test_execute_job_measures_runtime() -> None:
    delay_seconds = 0.05

    @register("slow")
    async def slow(_payload: dict) -> None:
        await asyncio.sleep(delay_seconds)

    outcome = await execute_job(_job(job_type="slow"))

    assert outcome.succeeded is True
    assert outcome.runtime_ms >= 10


async def test_execute_job_times_out() -> None:
    timeout_seconds = 1
    handler_sleep_seconds = 5

    @register("slow_timeout")
    async def slow_timeout(_payload: dict) -> None:
        await asyncio.sleep(handler_sleep_seconds)

    outcome = await execute_job(
        _job(job_type="slow_timeout", timeout_seconds=timeout_seconds)
    )

    assert outcome.succeeded is False
    assert outcome.result is None
    assert isinstance(outcome.error, JobTimeoutError)
    assert outcome.error.timeout_seconds == timeout_seconds
    assert str(outcome.error) == f"job timed out after {timeout_seconds}s"
    assert outcome.runtime_ms >= 500
    assert outcome.runtime_ms < handler_sleep_seconds * 1000


async def test_execute_job_zero_timeout_fails_immediately() -> None:
    @register("zero_timeout")
    async def zero_timeout(_payload: dict) -> None:
        await asyncio.sleep(5)

    outcome = await execute_job(_job(job_type="zero_timeout", timeout_seconds=0))

    assert outcome.succeeded is False
    assert isinstance(outcome.error, JobTimeoutError)
    assert outcome.error.timeout_seconds == 0
    assert outcome.runtime_ms < 5000


async def test_execute_job_does_not_swallow_cancellation() -> None:
    @register("cancel")
    async def cancel(_payload: dict) -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await execute_job(_job(job_type="cancel"))
