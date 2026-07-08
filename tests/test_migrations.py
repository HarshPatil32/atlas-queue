import subprocess
import sys
from collections.abc import Generator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config

from core.config import get_settings
from tests.conftest import DATABASE_URL


@pytest.fixture(autouse=True)
def migrations_test_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    monkeypatch.setenv("DATABASE_URL", DATABASE_URL)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _config() -> Config:
    return Config("alembic.ini")


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
