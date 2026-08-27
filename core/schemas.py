import json
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.models import JobAttemptStatus, JobStatus

DEFAULT_QUEUE = "default"
MAX_PAYLOAD_SIZE_BYTES = 256 * 1024
DEFAULT_LIMIT = 50
MAX_LIMIT = 100


class JobCreateRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    queue: str = Field(default=DEFAULT_QUEUE, min_length=1, max_length=255)
    job_type: str = Field(min_length=1, max_length=255)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=-1000, le=1000)
    max_retries: int = Field(default=3, ge=0)
    run_at: datetime | None = None
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=255)
    timeout_seconds: int = Field(default=60, gt=0)

    @field_validator("payload")
    @classmethod
    def payload_within_size_limit(cls, value: dict[str, Any]) -> dict[str, Any]:
        size = len(json.dumps(value, separators=(",", ":")).encode("utf-8"))
        if size > MAX_PAYLOAD_SIZE_BYTES:
            raise ValueError(
                f"payload exceeds maximum size of {MAX_PAYLOAD_SIZE_BYTES} bytes"
            )
        return value

    @field_validator("run_at")
    @classmethod
    def run_at_timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("run_at must be timezone-aware")
        return value


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    job_id: int = Field(validation_alias="id")
    queue: str
    job_type: str
    payload: dict[str, Any] = Field(validation_alias="payload_json")
    status: JobStatus
    priority: int
    attempts: int
    max_retries: int
    run_at: datetime = Field(validation_alias="next_run_at")
    timeout_seconds: int
    idempotency_key: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    failed_at: datetime | None
    dead_lettered_at: datetime | None
    last_error: str | None


class JobListResponse(BaseModel):
    jobs: list[JobResponse]
    total: int
    limit: int
    offset: int


class JobAttemptResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt_id: int = Field(validation_alias="id")
    attempt_number: int
    worker_id: str
    status: JobAttemptStatus
    started_at: datetime
    finished_at: datetime | None
    error_message: str | None
    runtime_ms: int | None


class DeadLetterJobDetailResponse(JobResponse):
    attempt_history: list[JobAttemptResponse]
