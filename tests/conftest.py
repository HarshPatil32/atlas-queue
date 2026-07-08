import pytest

# Structural-validation-only URL; tests do not connect to a real database.
DATABASE_URL = "postgresql+asyncpg://user:password@localhost:5432/atlas_queue"


@pytest.fixture
def database_url() -> str:
    return DATABASE_URL
