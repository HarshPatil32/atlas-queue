from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.db import Base


def _status_check_sql(status_enum: type[StrEnum]) -> str:
    values = ", ".join(f"'{status.value}'" for status in status_enum)
    return f"status IN ({values})"


class JobStatus(StrEnum):
    QUEUED = "queued"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"
    CANCELLED = "cancelled"


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(_status_check_sql(JobStatus), name="ck_jobs_status"),
        UniqueConstraint(
            "queue",
            "idempotency_key",
            name="uq_jobs_queue_idempotency_key",
        ),
        Index(
            "ix_jobs_queue_status_priority_next_run_at",
            "queue",
            text("priority DESC"),
            "next_run_at",
            "created_at",
            postgresql_where=text("status IN ('queued', 'scheduled', 'retrying')"),
        ),
        Index("ix_jobs_status_lease_expires_at", "status", "lease_expires_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    queue: Mapped[str] = mapped_column(String(255), nullable=False)
    job_type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=JobStatus.QUEUED.value,
        server_default=JobStatus.QUEUED.value,
    )
    priority: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    max_retries: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3,
        server_default="3",
    )
    next_run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    timeout_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=60,
        server_default="60",
    )
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class JobAttemptStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobAttempt(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (
        CheckConstraint(
            _status_check_sql(JobAttemptStatus),
            name="ck_job_attempts_status",
        ),
        UniqueConstraint(
            "job_id",
            "attempt_number",
            name="uq_job_attempts_job_id_attempt_number",
        ),
        Index("ix_job_attempts_worker_id", "worker_id"),
        Index("ix_job_attempts_status_started_at", "status", "started_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=JobAttemptStatus.RUNNING.value,
        server_default=JobAttemptStatus.RUNNING.value,
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    runtime_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)


class WorkerStatus(StrEnum):
    IDLE = "idle"
    RUNNING = "running"
    OFFLINE = "offline"


class Worker(Base):
    __tablename__ = "workers"
    __table_args__ = (
        CheckConstraint(_status_check_sql(WorkerStatus), name="ck_workers_status"),
        Index("ix_workers_status_last_heartbeat_at", "status", "last_heartbeat_at"),
        Index("ix_workers_current_job_id", "current_job_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    worker_name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    queues: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=WorkerStatus.IDLE.value,
        server_default=WorkerStatus.IDLE.value,
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    current_job_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("jobs.id", ondelete="SET NULL"),
        nullable=True,
    )
