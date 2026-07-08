from typing import cast

import pytest
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Table,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB

from core.models import Job, JobStatus


def _job_table() -> Table:
    return cast(Table, Job.__table__)


def _job_index(name: str) -> Index:
    for index in _job_table().indexes:
        if isinstance(index, Index) and index.name == name:
            return index
    raise AssertionError(f"Missing index: {name}")


def test_job_table_name() -> None:
    assert Job.__tablename__ == "jobs"


def test_job_status_values() -> None:
    assert len(JobStatus) == 8
    assert JobStatus.QUEUED.value == "queued"
    assert JobStatus.SCHEDULED.value == "scheduled"
    assert JobStatus.RUNNING.value == "running"
    assert JobStatus.SUCCEEDED.value == "succeeded"
    assert JobStatus.FAILED.value == "failed"
    assert JobStatus.RETRYING.value == "retrying"
    assert JobStatus.DEAD_LETTER.value == "dead_letter"
    assert JobStatus.CANCELLED.value == "cancelled"


@pytest.mark.parametrize(
    ("column_name", "expected_type", "nullable"),
    [
        ("id", BigInteger, False),
        ("queue", String, False),
        ("job_type", String, False),
        ("payload_json", JSONB, False),
        ("status", String, False),
        ("priority", Integer, False),
        ("attempts", Integer, False),
        ("max_retries", Integer, False),
        ("next_run_at", DateTime, False),
        ("timeout_seconds", Integer, False),
        ("idempotency_key", String, True),
        ("locked_by", String, True),
        ("locked_at", DateTime, True),
        ("lease_expires_at", DateTime, True),
        ("last_error", Text, True),
        ("created_at", DateTime, False),
        ("updated_at", DateTime, False),
        ("completed_at", DateTime, True),
        ("failed_at", DateTime, True),
    ],
)
def test_job_column_types_and_nullability(
    column_name: str,
    expected_type: type,
    nullable: bool,
) -> None:
    column = Job.__table__.columns[column_name]
    assert isinstance(column.type, expected_type)
    assert column.nullable is nullable


def test_job_id_is_primary_key() -> None:
    assert Job.__table__.columns["id"].primary_key is True


def test_job_python_column_defaults() -> None:
    assert Job.__table__.columns["payload_json"].default is not None
    assert Job.__table__.columns["status"].default is not None
    assert Job.__table__.columns["status"].default.arg == JobStatus.QUEUED.value
    assert Job.__table__.columns["priority"].default.arg == 0
    assert Job.__table__.columns["attempts"].default.arg == 0
    assert Job.__table__.columns["max_retries"].default.arg == 3
    assert Job.__table__.columns["timeout_seconds"].default.arg == 60


def test_job_nullable_fields_default_to_none_on_construct() -> None:
    job = Job(queue="default", job_type="send_email")
    assert job.idempotency_key is None
    assert job.locked_by is None
    assert job.locked_at is None
    assert job.lease_expires_at is None
    assert job.last_error is None
    assert job.completed_at is None
    assert job.failed_at is None


def test_job_scalar_server_defaults() -> None:
    assert Job.__table__.columns["status"].server_default is not None
    assert Job.__table__.columns["status"].server_default.arg == JobStatus.QUEUED.value
    assert Job.__table__.columns["priority"].server_default.arg == "0"
    assert Job.__table__.columns["attempts"].server_default.arg == "0"
    assert Job.__table__.columns["max_retries"].server_default.arg == "3"
    assert Job.__table__.columns["timeout_seconds"].server_default.arg == "60"


def test_job_timestamp_server_defaults() -> None:
    for column_name in ("next_run_at", "created_at", "updated_at"):
        column = Job.__table__.columns[column_name]
        assert column.server_default is not None


def test_job_idempotency_key_is_unique() -> None:
    column = Job.__table__.columns["idempotency_key"]
    assert column.unique is True


def test_job_status_check_constraint() -> None:
    constraints = [
        constraint
        for constraint in _job_table().constraints
        if isinstance(constraint, CheckConstraint)
    ]
    assert len(constraints) == 1
    constraint = constraints[0]
    assert constraint.name == "ck_jobs_status"
    # StrEnum preserves declaration order, which must match the constraint SQL.
    expected_values = ", ".join(f"'{status.value}'" for status in JobStatus)
    assert str(constraint.sqltext) == f"status IN ({expected_values})"


def test_job_polling_index() -> None:
    polling_index = _job_index("ix_jobs_queue_status_next_run_at")
    assert polling_index.columns.keys() == ["queue", "status", "next_run_at"]


def test_job_lease_reaper_index() -> None:
    lease_index = _job_index("ix_jobs_status_lease_expires_at")
    assert lease_index.columns.keys() == ["status", "lease_expires_at"]


def test_job_status_is_string_enum() -> None:
    assert issubclass(JobStatus, str)
    assert isinstance(JobStatus.QUEUED, str)


def test_job_timestamp_columns_are_timezone_aware() -> None:
    for column_name in (
        "next_run_at",
        "locked_at",
        "lease_expires_at",
        "created_at",
        "updated_at",
        "completed_at",
        "failed_at",
    ):
        column = Job.__table__.columns[column_name]
        assert isinstance(column.type, DateTime)
        assert column.type.timezone is True
