import pytest
from pydantic import ValidationError

from core.config import Settings, get_settings
from tests.conftest import DATABASE_URL


def _settings() -> Settings:
    return Settings(_env_file=None)


def test_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    settings = _settings()
    assert str(settings.database_url) == DATABASE_URL
    assert settings.lease_ttl_seconds == 30
    assert settings.poll_interval_seconds == 1.0
    assert settings.worker_concurrency == 4
    assert settings.log_level == "INFO"
    assert settings.log_format == "json"


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("LEASE_TTL_SECONDS", "60")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "2.5")
    monkeypatch.setenv("WORKER_CONCURRENCY", "8")
    settings = _settings()
    assert settings.lease_ttl_seconds == 60
    assert settings.poll_interval_seconds == 2.5
    assert settings.worker_concurrency == 8


def test_missing_database_url_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValidationError):
        _settings()


def test_invalid_lease_ttl_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("LEASE_TTL_SECONDS", "0")
    with pytest.raises(ValidationError):
        _settings()


def test_invalid_poll_interval_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0")
    with pytest.raises(ValidationError):
        _settings()


def test_invalid_concurrency_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("WORKER_CONCURRENCY", "0")
    with pytest.raises(ValidationError):
        _settings()


def test_get_settings_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    get_settings.cache_clear()
    first = get_settings()
    second = get_settings()
    assert first is second


def test_log_format_accepts_json_and_console(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    assert _settings().log_format == "json"

    monkeypatch.setenv("LOG_FORMAT", "console")
    assert _settings().log_format == "console"


def test_invalid_log_format_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("LOG_FORMAT", "yaml")
    with pytest.raises(ValidationError):
        _settings()


def test_invalid_log_level_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    monkeypatch.setenv("LOG_LEVEL", "VERBOSELY")
    with pytest.raises(ValidationError):
        _settings()
