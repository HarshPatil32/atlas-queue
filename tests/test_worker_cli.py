import pytest

from core.config import Settings
from core.schemas import DEFAULT_QUEUE
from tests.conftest import DATABASE_URL
from worker.cli import MAX_NAME_LENGTH, default_worker_name, parse_args


def _settings() -> Settings:
    return Settings(_env_file=None)


def test_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    args = parse_args([], settings=_settings())
    assert args.queues == [DEFAULT_QUEUE]
    assert args.concurrency == 4
    assert args.name is None


def test_queues_parsed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    args = parse_args(["--queues", "a", "b", "c"], settings=_settings())
    assert args.queues == ["a", "b", "c"]


def test_concurrency_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    args = parse_args(["--concurrency", "8"], settings=_settings())
    assert args.concurrency == 8


def test_name_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    args = parse_args(["--name", "worker-1"], settings=_settings())
    assert args.name == "worker-1"


def test_default_worker_name_format() -> None:
    name = default_worker_name()
    assert "-" in name
    assert name.endswith(str(__import__("os").getpid()))


def test_invalid_concurrency_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    with pytest.raises(SystemExit):
        parse_args(["--concurrency", "0"], settings=_settings())


def test_negative_concurrency_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    with pytest.raises(SystemExit):
        parse_args(["--concurrency", "-1"], settings=_settings())


def test_empty_queue_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    with pytest.raises(SystemExit):
        parse_args(["--queues", ""], settings=_settings())


def test_overlong_queue_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    with pytest.raises(SystemExit):
        parse_args(["--queues", "a" * (MAX_NAME_LENGTH + 1)], settings=_settings())


def test_empty_name_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    with pytest.raises(SystemExit):
        parse_args(["--name", ""], settings=_settings())


def test_overlong_name_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    with pytest.raises(SystemExit):
        parse_args(["--name", "a" * (MAX_NAME_LENGTH + 1)], settings=_settings())


def test_invalid_concurrency_type_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    with pytest.raises(SystemExit):
        parse_args(["--concurrency", "not-a-number"], settings=_settings())
