from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.models import JobStatus


class JobCreateRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    queue: str = Field(min_length=1, max_length=255)
    job_type: str = Field(min_length=1, max_length=255)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0
    max_retries: int = Field(default=3, ge=0)
    run_at: datetime | None = None
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=255)
    timeout_seconds: int = Field(default=60, gt=0)

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
    last_error: str | None
