import asyncio
import subprocess
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any, TypedDict

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, pool
from sqlalchemy.ext.asyncio import create_async_engine

from core.config import get_settings
from tests.conftest import DATABASE_URL
from tests.live_postgres import (
    DEV_TEST_POSTGRES_COMMAND,
    get_live_test_database_url,
    postgres_is_reachable,
)

EXPECTED_TABLES = frozenset({"jobs", "job_attempts", "workers"})


class MigratedSchema(TypedDict):
    table_names: set[str]
    jobs_indexes: set[str]
    jobs_unique_constraints: list[dict[str, Any]]
    job_attempt_fks: list[dict[str, Any]]
    worker_fks: list[dict[str, Any]]
    jobs_checks: set[str]
    job_attempt_checks: set[str]
    worker_checks: set[str]


@pytest.fixture(autouse=True)
def migrations_test_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _config() -> Config:
    return Config("alembic.ini")


def _inspect_migrated_schema(connection) -> MigratedSchema:
    inspector = inspect(connection)
    table_names = set(inspector.get_table_names())
    jobs_indexes = {index["name"] for index in inspector.get_indexes("jobs")}
    jobs_unique_constraints = inspector.get_unique_constraints("jobs")
    job_attempt_fks = inspector.get_foreign_keys("job_attempts")
    worker_fks = inspector.get_foreign_keys("workers")
    jobs_checks = {check["name"] for check in inspector.get_check_constraints("jobs")}
    job_attempt_checks = {
        check["name"] for check in inspector.get_check_constraints("job_attempts")
    }
    worker_checks = {
        check["name"] for check in inspector.get_check_constraints("workers")
    }
    return {
        "table_names": table_names,
        "jobs_indexes": jobs_indexes,
        "jobs_unique_constraints": jobs_unique_constraints,
        "job_attempt_fks": job_attempt_fks,
        "worker_fks": worker_fks,
        "jobs_checks": jobs_checks,
        "job_attempt_checks": job_attempt_checks,
        "worker_checks": worker_checks,
    }


async def _postgres_is_reachable(database_url: str) -> bool:
    return await postgres_is_reachable(database_url)


async def _read_schema(database_url: str) -> MigratedSchema:
    engine = create_async_engine(database_url, poolclass=pool.NullPool)
    try:
        async with engine.connect() as connection:
            return await connection.run_sync(_inspect_migrated_schema)
    finally:
        await engine.dispose()


async def _read_table_names(database_url: str) -> set[str]:
    engine = create_async_engine(database_url, poolclass=pool.NullPool)
    try:
        async with engine.connect() as connection:

            def _table_names(sync_connection) -> set[str]:
                return set(inspect(sync_connection).get_table_names())

            return await connection.run_sync(_table_names)
    finally:
        await engine.dispose()


def _assert_upgraded_schema(schema: MigratedSchema) -> None:
    table_names = schema["table_names"]
    assert EXPECTED_TABLES.issubset(table_names)

    jobs_indexes = schema["jobs_indexes"]
    assert "ix_jobs_queue_status_priority_next_run_at" in jobs_indexes
    assert "ix_jobs_status_lease_expires_at" in jobs_indexes

    jobs_unique_constraints = schema["jobs_unique_constraints"]
    assert len(jobs_unique_constraints) == 1
    jobs_unique_constraint = jobs_unique_constraints[0]
    assert jobs_unique_constraint["name"] == "uq_jobs_queue_idempotency_key"
    assert jobs_unique_constraint["column_names"] == ["queue", "idempotency_key"]

    job_attempt_fks = schema["job_attempt_fks"]
    assert len(job_attempt_fks) == 1
    job_attempt_fk = job_attempt_fks[0]
    assert job_attempt_fk["referred_table"] == "jobs"
    assert job_attempt_fk["referred_columns"] == ["id"]
    assert job_attempt_fk["constrained_columns"] == ["job_id"]
    assert job_attempt_fk["options"].get("ondelete") == "CASCADE"

    worker_fks = schema["worker_fks"]
    assert len(worker_fks) == 1
    worker_fk = worker_fks[0]
    assert worker_fk["referred_table"] == "jobs"
    assert worker_fk["referred_columns"] == ["id"]
    assert worker_fk["constrained_columns"] == ["current_job_id"]
    assert worker_fk["options"].get("ondelete") == "SET NULL"

    assert schema["jobs_checks"] == {"ck_jobs_status"}
    assert schema["job_attempt_checks"] == {"ck_job_attempts_status"}
    assert schema["worker_checks"] == {"ck_workers_status"}


def test_upgrade_head_and_downgrade_base_against_live_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run live migration verification against a disposable Postgres database.

    Set LIVE_MIGRATIONS_TEST_DATABASE_URL to override the default dev test
    database, or start the bundled container with:
    ./scripts/dev-test-postgres.sh up

    Default URL:
    postgresql+asyncpg://atlas:atlas@localhost:5434/atlas_queue_test

    Do not point this at a database that holds data you need to keep. The test
    runs `alembic downgrade base`, which drops all application tables.
    """
    live_url = get_live_test_database_url()
    if not asyncio.run(_postgres_is_reachable(live_url)):
        pytest.skip(
            f"Live Postgres is not reachable at {live_url}. "
            f"Start the dev test database with: {DEV_TEST_POSTGRES_COMMAND}"
        )

    monkeypatch.setenv("DATABASE_URL", live_url)
    # Alembic env.py reads get_settings(); clear cache so it picks up live_url.
    get_settings.cache_clear()

    config = _config()
    command.downgrade(config, "base")
    command.upgrade(config, "head")

    upgraded_schema = asyncio.run(_read_schema(live_url))
    _assert_upgraded_schema(upgraded_schema)

    command.downgrade(config, "base")

    remaining_tables = asyncio.run(_read_table_names(live_url))
    assert not EXPECTED_TABLES.intersection(remaining_tables)


def test_alembic_config_loads_and_points_at_migrations_dir() -> None:
    config = _config()
    script_location = config.get_main_option("script_location")
    assert script_location is not None
    assert Path(script_location).name == "migrations"


def test_upgrade_head_offline_sql_runs_without_connecting_to_a_database() -> None:
    command.upgrade(_config(), "head", sql=True)


def test_downgrade_base_offline_sql_runs_without_connecting_to_a_database() -> None:
    command.downgrade(_config(), "head:base", sql=True)


def test_revision_template_renders_ruff_clean_script(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    versions_dir = migrations_dir / "versions"
    versions_dir.mkdir(parents=True)

    project_root = Path(__file__).resolve().parents[1]
    (migrations_dir / "script.py.mako").write_text(
        (project_root / "migrations" / "script.py.mako").read_text()
    )
    (migrations_dir / "env.py").write_text("from alembic import context\n")

    config = Config()
    config.set_main_option("script_location", str(migrations_dir))
    command.revision(config, message="template test")

    revision_files = list(versions_dir.glob("*.py"))
    assert len(revision_files) == 1
    revision_path = revision_files[0]

    fix_result = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "--fix", str(revision_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert fix_result.returncode == 0, fix_result.stdout + fix_result.stderr

    black_result = subprocess.run(
        [sys.executable, "-m", "black", str(revision_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert black_result.returncode == 0, black_result.stdout + black_result.stderr

    result = subprocess.run(
        [sys.executable, "-m", "ruff", "check", str(revision_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
