"""In-process task registry mapping job_type strings to async handlers.

Handlers must be registered (their module imported) before the worker starts
polling. This module does not auto-discover task modules.
"""

from collections.abc import Awaitable, Callable
from typing import Any

TaskHandler = Callable[[dict[str, Any]], Awaitable[Any]]

_registry: dict[str, TaskHandler] = {}


class UnknownJobTypeError(KeyError):
    def __init__(self, job_type: str) -> None:
        self.job_type = job_type
        super().__init__(f"unknown job_type: {job_type!r}")

    def __str__(self) -> str:
        return str(self.args[0])


def register(job_type: str) -> Callable[[TaskHandler], TaskHandler]:
    # Strips whitespace on register; get_handler expects the exact stored key.
    normalized = job_type.strip()
    if not normalized:
        raise ValueError("job_type must be non-empty")

    def decorator(handler: TaskHandler) -> TaskHandler:
        if normalized in _registry:
            raise ValueError(f"job_type already registered: {normalized!r}")
        _registry[normalized] = handler
        return handler

    return decorator


def get_handler(job_type: str) -> TaskHandler:
    try:
        return _registry[job_type]
    except KeyError as exc:
        raise UnknownJobTypeError(job_type) from exc


def clear_registry() -> None:
    """Reset the registry. Intended for tests only."""
    _registry.clear()
