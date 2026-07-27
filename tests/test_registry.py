from collections.abc import Iterator

import pytest

from core.registry import UnknownJobTypeError, clear_registry, get_handler, register


@pytest.fixture(autouse=True)
def _clean_registry() -> Iterator[None]:
    clear_registry()
    yield
    clear_registry()


async def test_register_and_get_handler_round_trip() -> None:
    @register("echo")
    async def echo(payload: dict) -> dict:
        return payload

    assert get_handler("echo") is echo


async def test_decorator_returns_original_function() -> None:
    async def handler(payload: dict[str, str]) -> str:
        return payload["value"]

    decorated = register("my_task")(handler)
    assert decorated is handler
    assert await decorated({"value": "ok"}) == "ok"


def test_empty_job_type_raises() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        register("")


def test_whitespace_job_type_raises() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        register("   ")


def test_duplicate_registration_raises() -> None:
    @register("dup")
    async def first(_payload: dict) -> None:
        return None

    with pytest.raises(ValueError, match="already registered"):

        @register("dup")
        async def second(_payload: dict) -> None:
            return None

    assert get_handler("dup") is first


def test_unknown_job_type_raises() -> None:
    with pytest.raises(UnknownJobTypeError) as exc_info:
        get_handler("missing")

    assert exc_info.value.job_type == "missing"
    assert str(exc_info.value) == "unknown job_type: 'missing'"


def test_unknown_job_type_is_a_key_error() -> None:
    with pytest.raises(KeyError):
        get_handler("missing")


async def test_handler_executes_with_payload() -> None:
    @register("add")
    async def add(payload: dict[str, int]) -> int:
        return payload["a"] + payload["b"]

    result = await get_handler("add")({"a": 2, "b": 3})
    assert result == 5
