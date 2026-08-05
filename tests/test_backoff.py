from collections.abc import Iterator

import pytest

from core.backoff import compute_backoff_seconds, retry_delay_seconds
from core.config import get_settings
from tests.conftest import DATABASE_URL


@pytest.fixture(autouse=True)
def clear_settings_cache() -> Iterator[None]:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [
        (1, 10.0),
        (2, 30.0),
        (3, 90.0),
        (4, 270.0),
        (5, 300.0),
        (100, 300.0),
    ],
)
def test_compute_backoff_seconds(attempt: int, expected: float) -> None:
    assert (
        compute_backoff_seconds(
            attempt,
            base_delay_seconds=10.0,
            multiplier=3.0,
            max_delay_seconds=300.0,
        )
        == expected
    )


@pytest.mark.parametrize("attempt", [0, -1])
def test_compute_backoff_seconds_rejects_invalid_attempt(attempt: int) -> None:
    with pytest.raises(ValueError, match="attempt must be >= 1"):
        compute_backoff_seconds(
            attempt,
            base_delay_seconds=10.0,
            multiplier=3.0,
            max_delay_seconds=300.0,
        )


def test_compute_backoff_seconds_constant_when_multiplier_is_one() -> None:
    for attempt in (1, 2, 10):
        assert (
            compute_backoff_seconds(
                attempt,
                base_delay_seconds=10.0,
                multiplier=1.0,
                max_delay_seconds=300.0,
            )
            == 10.0
        )


def test_compute_backoff_seconds_handles_large_attempt_without_overflow() -> None:
    assert (
        compute_backoff_seconds(
            10_000,
            base_delay_seconds=10.0,
            multiplier=3.0,
            max_delay_seconds=300.0,
        )
        == 300.0
    )


def test_retry_delay_seconds_reads_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("RETRY_BACKOFF_BASE_SECONDS", "5.0")
    monkeypatch.setenv("RETRY_BACKOFF_MULTIPLIER", "2.0")
    monkeypatch.setenv("RETRY_BACKOFF_MAX_SECONDS", "40.0")

    expected = compute_backoff_seconds(
        3,
        base_delay_seconds=5.0,
        multiplier=2.0,
        max_delay_seconds=40.0,
    )
    assert retry_delay_seconds(3) == expected
