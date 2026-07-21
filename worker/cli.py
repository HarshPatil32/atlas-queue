import argparse
import os
import socket
from dataclasses import dataclass

from core.config import Settings, get_settings
from core.schemas import DEFAULT_QUEUE

MAX_NAME_LENGTH = 255


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid int value: {value!r}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("concurrency must be > 0")
    return parsed


def _validate_queue(value: str) -> str:
    queue = value.strip()
    if not queue:
        raise argparse.ArgumentTypeError("queue name must be non-empty")
    if len(queue) > MAX_NAME_LENGTH:
        raise argparse.ArgumentTypeError(
            f"queue name must be at most {MAX_NAME_LENGTH} characters"
        )
    return queue


def _validate_name(value: str) -> str:
    name = value.strip()
    if not name:
        raise argparse.ArgumentTypeError("worker name must be non-empty")
    if len(name) > MAX_NAME_LENGTH:
        raise argparse.ArgumentTypeError(
            f"worker name must be at most {MAX_NAME_LENGTH} characters"
        )
    return name


@dataclass(frozen=True)
class WorkerArgs:
    queues: list[str]
    concurrency: int
    name: str | None


def default_worker_name() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def parse_args(
    argv: list[str] | None = None,
    *,
    settings: Settings | None = None,
) -> WorkerArgs:
    if settings is None:
        settings = get_settings()

    parser = argparse.ArgumentParser(description="Atlas queue worker")
    parser.add_argument(
        "--queues",
        nargs="+",
        type=_validate_queue,
        default=[DEFAULT_QUEUE],
        help="Queue names to process (default: default)",
    )
    parser.add_argument(
        "--concurrency",
        type=_positive_int,
        default=settings.worker_concurrency,
        help=f"Concurrent job slots (default: {settings.worker_concurrency})",
    )
    parser.add_argument(
        "--name",
        type=_validate_name,
        default=None,
        help="Worker name (default: hostname-pid)",
    )

    namespace = parser.parse_args(argv)
    return WorkerArgs(
        queues=namespace.queues,
        concurrency=namespace.concurrency,
        name=namespace.name,
    )
