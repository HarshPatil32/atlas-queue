import logging

import pytest
import structlog
from structlog.testing import capture_logs

from core.logging import (
    bind_request_context,
    bind_worker_context,
    clear_context,
    configure_logging,
    get_logger,
)


@pytest.fixture(autouse=True)
def reset_logging():
    clear_context()
    structlog.reset_defaults()
    root = logging.getLogger()
    root.handlers = []
    root.setLevel(logging.WARNING)
    yield
    clear_context()
    structlog.reset_defaults()
    root.handlers = []
    root.setLevel(logging.WARNING)


def test_configure_logging_runs_without_error() -> None:
    configure_logging(log_level="DEBUG", log_format="json")
    configure_logging(log_level="INFO", log_format="console")


def test_get_logger_returns_bound_logger() -> None:
    configure_logging()
    with capture_logs() as cap_logs:
        get_logger("test").info("ready")
    assert cap_logs[0]["event"] == "ready"


def test_configure_logging_sets_root_level() -> None:
    configure_logging(log_level="DEBUG")
    assert logging.getLogger().level == logging.DEBUG


def test_configure_logging_rejects_invalid_log_level() -> None:
    with pytest.raises(ValueError, match="log_level must be one of"):
        configure_logging(log_level="VERBOSELY")


def test_bind_request_context() -> None:
    bind_request_context(
        request_id="req-1",
        method="GET",
        path="/jobs",
    )
    with capture_logs(processors=[structlog.contextvars.merge_contextvars]) as cap_logs:
        get_logger("test").info("handled")
    assert cap_logs[0]["request_id"] == "req-1"
    assert cap_logs[0]["method"] == "GET"
    assert cap_logs[0]["path"] == "/jobs"


def test_bind_worker_context() -> None:
    bind_worker_context(
        worker_id="worker-1",
        job_id="job-42",
        job_type="sleep_job",
        queue="default",
    )
    with capture_logs(processors=[structlog.contextvars.merge_contextvars]) as cap_logs:
        get_logger("test").info("executing")
    assert cap_logs[0]["worker_id"] == "worker-1"
    assert cap_logs[0]["job_id"] == "job-42"
    assert cap_logs[0]["job_type"] == "sleep_job"
    assert cap_logs[0]["queue"] == "default"


def test_bind_worker_context_omits_none_fields() -> None:
    bind_worker_context(worker_id="worker-1")
    with capture_logs(processors=[structlog.contextvars.merge_contextvars]) as cap_logs:
        get_logger("test").info("idle")
    assert cap_logs[0]["worker_id"] == "worker-1"
    assert "job_id" not in cap_logs[0]
    assert "job_type" not in cap_logs[0]
    assert "queue" not in cap_logs[0]


def test_clear_context_removes_bound_fields() -> None:
    bind_request_context(
        request_id="req-1",
        method="POST",
        path="/jobs",
    )
    clear_context()
    with capture_logs() as cap_logs:
        get_logger("test").info("handled")
    assert "request_id" not in cap_logs[0]
    assert "method" not in cap_logs[0]
    assert "path" not in cap_logs[0]
