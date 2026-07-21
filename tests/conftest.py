from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from api.deps import get_db
from api.main import app
from core.db import Base
from tests.live_postgres import drop_metadata_tables, require_live_postgres_async

# Structural-validation-only URL; tests do not connect to a real database.
DATABASE_URL = "postgresql+asyncpg://user:password@localhost:5432/atlas_queue"


@pytest.fixture
def database_url() -> str:
    return DATABASE_URL


@pytest.fixture
async def live_client() -> (
    AsyncIterator[tuple[AsyncClient, async_sessionmaker[AsyncSession]]]
):
    live_url = await require_live_postgres_async()

    engine = create_async_engine(live_url, poolclass=pool.NullPool)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with sessionmaker() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client, sessionmaker

    app.dependency_overrides.pop(get_db, None)
    await drop_metadata_tables(engine)
    await engine.dispose()
