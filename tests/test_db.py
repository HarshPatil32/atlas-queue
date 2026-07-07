from collections.abc import Generator

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from core.config import get_settings
from core.db import (
    Base,
    dispose_engine,
    get_engine,
    get_session,
    get_sessionmaker,
)
from tests.conftest import DATABASE_URL


@pytest.fixture(autouse=True)
def db_test_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_sessionmaker.cache_clear()
    yield
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_sessionmaker.cache_clear()


def test_get_engine_uses_settings_url() -> None:
    engine = get_engine()
    assert isinstance(engine, AsyncEngine)
    assert engine.url.drivername == "postgresql+asyncpg"
    assert engine.url.username == "user"
    assert engine.url.host == "localhost"
    assert engine.url.port == 5432
    assert engine.url.database == "atlas_queue"


def test_get_engine_echo_follows_log_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    assert get_engine().echo is False

    get_engine.cache_clear()
    get_settings.cache_clear()
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    assert get_engine().echo is True


def test_get_sessionmaker_bound_to_engine() -> None:
    sessionmaker = get_sessionmaker()
    assert isinstance(sessionmaker, async_sessionmaker)
    assert sessionmaker.kw["expire_on_commit"] is False
    assert sessionmaker.kw["bind"] is get_engine()


def test_get_engine_is_cached() -> None:
    assert get_engine() is get_engine()


def test_get_sessionmaker_is_cached() -> None:
    assert get_sessionmaker() is get_sessionmaker()


def test_base_is_declarative_base() -> None:
    assert issubclass(Base, DeclarativeBase)


async def test_get_session_yields_async_session() -> None:
    async with get_session() as session:
        assert isinstance(session, AsyncSession)


async def test_dispose_engine_clears_caches() -> None:
    engine = get_engine()
    await dispose_engine()
    assert get_engine() is not engine


async def test_dispose_engine_noop_when_engine_never_built() -> None:
    await dispose_engine()
    assert get_engine.cache_info().currsize == 0
