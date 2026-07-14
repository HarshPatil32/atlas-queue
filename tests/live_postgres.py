import asyncio
import os

import pytest
from sqlalchemy import pool, text
from sqlalchemy.ext.asyncio import create_async_engine

LIVE_MIGRATIONS_TEST_DATABASE_URL = "LIVE_MIGRATIONS_TEST_DATABASE_URL"
DEFAULT_LIVE_TEST_DATABASE_URL = (
    "postgresql+asyncpg://atlas:atlas@localhost:5434/atlas_queue_test"
)
DEV_TEST_POSTGRES_COMMAND = "./scripts/dev-test-postgres.sh up"


def get_live_test_database_url() -> str:
    return os.environ.get(
        LIVE_MIGRATIONS_TEST_DATABASE_URL,
        DEFAULT_LIVE_TEST_DATABASE_URL,
    )


async def postgres_is_reachable(database_url: str) -> bool:
    engine = create_async_engine(database_url, poolclass=pool.NullPool)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
    finally:
        await engine.dispose()


def require_live_postgres() -> str:
    """For sync tests only. Use require_live_postgres_async in async fixtures."""
    live_url = get_live_test_database_url()
    if not asyncio.run(postgres_is_reachable(live_url)):
        pytest.skip(
            f"Live Postgres is not reachable at {live_url}. "
            f"Start the dev test database with: {DEV_TEST_POSTGRES_COMMAND}"
        )
    return live_url


async def require_live_postgres_async() -> str:
    live_url = get_live_test_database_url()
    if not await postgres_is_reachable(live_url):
        pytest.skip(
            f"Live Postgres is not reachable at {live_url}. "
            f"Start the dev test database with: {DEV_TEST_POSTGRES_COMMAND}"
        )
    return live_url
