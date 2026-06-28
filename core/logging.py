import logging
import sys
from typing import Literal, cast

import structlog

LogFormat = Literal["json", "console"]


def configure_logging(log_level: str = "INFO", log_format: LogFormat = "json") -> None:
    normalized_level = log_level.upper()
    if normalized_level not in logging.getLevelNamesMapping():
        valid = ", ".join(sorted(logging.getLevelNamesMapping()))
        raise ValueError(f"log_level must be one of: {valid}")

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if log_format == "console":
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.reset_defaults()
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(normalized_level)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name))


def bind_request_context(request_id: str, method: str, path: str) -> None:
    """Bind HTTP request fields for the current async context.

    Call from request middleware; call clear_context() when the request ends.
    request_id must be generated server-side (e.g. uuid4), never from client input.
    """
    structlog.contextvars.bind_contextvars(
        request_id=request_id,
        method=method,
        path=path,
    )


def bind_worker_context(
    *,
    worker_id: str,
    job_id: str | None = None,
    job_type: str | None = None,
    queue: str | None = None,
) -> None:
    """Bind worker/job fields for the current async context.

    Call when a job is claimed; call clear_context() when execution finishes.
    Optional fields omitted when None so JSON logs stay clean.
    """
    context = {
        "worker_id": worker_id,
        "job_id": job_id,
        "job_type": job_type,
        "queue": queue,
    }
    structlog.contextvars.bind_contextvars(
        **{key: value for key, value in context.items() if value is not None}
    )


def clear_context() -> None:
    structlog.contextvars.clear_contextvars()
