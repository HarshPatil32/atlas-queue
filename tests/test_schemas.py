from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from core.models import Job, JobStatus
from core.schemas import JobCreateRequest, JobResponse


def test_job_create_request_minimal_valid_body_uses_defaults() -> None:
    request = JobCreateRequest(queue="default", job_type="send_email")

    assert request.queue == "default"
    assert request.job_type == "send_email"
    assert request.payload == {}
    assert request.priority == 0
    assert request.max_retries == 3
    assert request.run_at is None
    assert request.idempotency_key is None
    assert request.timeout_seconds == 60


@pytest.mark.parametrize("queue", ["", "   "])
def test_job_create_request_rejects_empty_or_whitespace_queue(queue: str) -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest(queue=queue, job_type="send_email")


@pytest.mark.parametrize("job_type", ["", "   "])
def test_job_create_request_rejects_empty_or_whitespace_job_type(
    job_type: str,
) -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest(queue="default", job_type=job_type)


def test_job_create_request_strips_whitespace_from_queue_and_job_type() -> None:
    request = JobCreateRequest(queue="  default  ", job_type="  send_email  ")

    assert request.queue == "default"
    assert request.job_type == "send_email"


def test_job_create_request_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {"queue": "default", "job_type": "send_email", "unknown": "field"}
        )


def test_job_create_request_accepts_zero_max_retries() -> None:
    request = JobCreateRequest(
        queue="default",
        job_type="send_email",
        max_retries=0,
    )

    assert request.max_retries == 0


@pytest.mark.parametrize("max_retries", [-1, -10])
def test_job_create_request_rejects_negative_max_retries(max_retries: int) -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest(
            queue="default",
            job_type="send_email",
            max_retries=max_retries,
        )


@pytest.mark.parametrize("timeout_seconds", [0, -1])
def test_job_create_request_rejects_non_positive_timeout(
    timeout_seconds: int,
) -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest(
            queue="default",
            job_type="send_email",
            timeout_seconds=timeout_seconds,
        )


@pytest.mark.parametrize("payload", [[], "not-a-dict", 123])
def test_job_create_request_rejects_non_dict_payload(payload: object) -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest(
            queue="default",
            job_type="send_email",
            payload=payload,
        )


def test_job_create_request_rejects_naive_run_at() -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest(
            queue="default",
            job_type="send_email",
            run_at=datetime(2026, 1, 1, 12, 0, 0),
        )


def test_job_create_request_accepts_timezone_aware_run_at() -> None:
    run_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    request = JobCreateRequest(
        queue="default",
        job_type="send_email",
        run_at=run_at,
    )

    assert request.run_at == run_at


def test_job_create_request_idempotency_key_defaults_to_none() -> None:
    request = JobCreateRequest(queue="default", job_type="send_email")

    assert request.idempotency_key is None


@pytest.mark.parametrize("idempotency_key", ["", "   "])
def test_job_create_request_rejects_empty_or_whitespace_idempotency_key(
    idempotency_key: str,
) -> None:
    with pytest.raises(ValidationError):
        JobCreateRequest(
            queue="default",
            job_type="send_email",
            idempotency_key=idempotency_key,
        )


def test_job_create_request_accepts_valid_idempotency_key() -> None:
    request = JobCreateRequest(
        queue="default",
        job_type="send_email",
        idempotency_key="create-order-123",
    )

    assert request.idempotency_key == "create-order-123"


def test_job_response_constructible_by_field_names() -> None:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    response = JobResponse(
        job_id=1,
        queue="default",
        job_type="send_email",
        payload={"to": "user@example.com"},
        status=JobStatus.QUEUED,
        priority=0,
        attempts=0,
        max_retries=3,
        run_at=now,
        timeout_seconds=60,
        idempotency_key=None,
        created_at=now,
        updated_at=now,
        completed_at=None,
        failed_at=None,
        last_error=None,
    )

    assert response.job_id == 1
    assert response.payload == {"to": "user@example.com"}
    assert response.run_at == now


def test_job_response_model_validate_maps_orm_fields() -> None:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    job = Job(
        id=42,
        queue="default",
        job_type="send_email",
        payload_json={"to": "user@example.com"},
        status=JobStatus.QUEUED.value,
        priority=5,
        attempts=1,
        max_retries=3,
        next_run_at=now,
        timeout_seconds=60,
        idempotency_key="create-order-123",
        created_at=now,
        updated_at=now,
        completed_at=None,
        failed_at=None,
        last_error=None,
    )

    response = JobResponse.model_validate(job)

    assert response.job_id == 42
    assert response.payload == {"to": "user@example.com"}
    assert response.run_at == now
    assert response.status == JobStatus.QUEUED
    assert response.priority == 5
    assert response.attempts == 1
    assert response.idempotency_key == "create-order-123"


def test_job_response_status_serializes_to_string() -> None:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    response = JobResponse(
        job_id=1,
        queue="default",
        job_type="send_email",
        payload={},
        status=JobStatus.QUEUED,
        priority=0,
        attempts=0,
        max_retries=3,
        run_at=now,
        timeout_seconds=60,
        idempotency_key=None,
        created_at=now,
        updated_at=now,
        completed_at=None,
        failed_at=None,
        last_error=None,
    )

    dumped = response.model_dump(mode="json")

    assert dumped["status"] == "queued"
