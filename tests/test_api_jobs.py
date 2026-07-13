from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from api.deps import get_db
from api.main import app
from core.models import Job, JobStatus
from core.schemas import DEFAULT_LIMIT, MAX_LIMIT


def _make_job(
    *,
    job_id: int = 1,
    job_status: str = JobStatus.RUNNING.value,
    last_error: str | None = None,
    idempotency_key: str | None = "order-123",
) -> Job:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    return Job(
        id=job_id,
        queue="emails",
        job_type="send_email",
        payload_json={"to": "user@example.com"},
        status=job_status,
        priority=5,
        attempts=2,
        max_retries=3,
        next_run_at=datetime(2026, 6, 1, 15, 30, 0, tzinfo=UTC),
        timeout_seconds=120,
        idempotency_key=idempotency_key,
        locked_by="worker-1",
        locked_at=now,
        lease_expires_at=datetime(2026, 1, 1, 12, 5, 0, tzinfo=UTC),
        last_error=last_error,
        created_at=now,
        updated_at=now,
        completed_at=None,
        failed_at=None,
    )


class FakeResult:
    def __init__(
        self,
        *,
        scalar: int | None = None,
        rows: list[Job] | None = None,
    ) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one(self) -> int:
        assert self._scalar is not None
        return self._scalar

    def scalars(self) -> "FakeResult":
        return self

    def all(self) -> list[Job]:
        return self._rows

    def one_or_none(self) -> Job | None:
        if not self._rows:
            return None
        return self._rows[0]


class FakeSession:
    def __init__(self) -> None:
        self.added: list[Job] = []
        self.jobs: dict[int, Job] = {}
        self.committed = False
        self.next_run_at_unset_before_refresh: list[bool] = []
        self.list_jobs_rows: list[Job] = []
        self.list_jobs_total = 0
        self.execute_call_count = 0
        self.update_returning_rows: list[Job] = []

    def add(self, job: Job) -> None:
        self.added.append(job)

    async def get(self, model: type, job_id: int) -> Job | None:
        return self.jobs.get(job_id)

    async def commit(self) -> None:
        self.committed = True

    async def refresh(self, job: Job) -> None:
        self.next_run_at_unset_before_refresh.append(job.next_run_at is None)
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        if job.id is None:
            job.id = 1
        if job.status is None:
            job.status = JobStatus.QUEUED.value
        if job.attempts is None:
            job.attempts = 0
        if job.created_at is None:
            job.created_at = now
        if job.updated_at is None:
            job.updated_at = now
        if job.next_run_at is None:
            job.next_run_at = now

    async def execute(self, statement: Any) -> FakeResult:
        # Does not simulate SQL filtering; returns canned rows for envelope tests only.
        # list_jobs issues count first, then select; use call order instead of
        # inspecting SQLAlchemy internals.
        if getattr(statement, "is_update", False):
            return FakeResult(rows=self.update_returning_rows)

        self.execute_call_count += 1
        if self.execute_call_count == 1:
            return FakeResult(scalar=self.list_jobs_total)
        return FakeResult(rows=self.list_jobs_rows)


@pytest.fixture
async def client_with_session() -> AsyncIterator[tuple[AsyncClient, FakeSession]]:
    fake_session = FakeSession()

    async def override_get_db() -> AsyncIterator[FakeSession]:
        yield fake_session

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client, fake_session
    app.dependency_overrides.pop(get_db, None)


async def test_create_job_returns_201_with_job_id_status_created_at(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    response = await http_client.post(
        "/jobs",
        json={"queue": "default", "job_type": "send_email"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["job_id"] == 1
    assert body["status"] == "queued"
    assert body["created_at"] == "2026-01-01T12:00:00Z"
    assert body["queue"] == "default"
    assert body["job_type"] == "send_email"
    assert body["payload"] == {}
    assert fake_session.next_run_at_unset_before_refresh == [True]


async def test_create_job_maps_request_fields_to_job(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    run_at = "2026-06-01T15:30:00Z"

    response = await http_client.post(
        "/jobs",
        json={
            "queue": "emails",
            "job_type": "send_email",
            "payload": {"to": "user@example.com"},
            "priority": 5,
            "max_retries": 2,
            "run_at": run_at,
            "idempotency_key": "order-123",
            "timeout_seconds": 120,
        },
    )

    assert response.status_code == 201
    assert fake_session.committed is True
    assert len(fake_session.added) == 1
    assert fake_session.next_run_at_unset_before_refresh == [False]

    job = fake_session.added[0]
    assert job.queue == "emails"
    assert job.job_type == "send_email"
    assert job.payload_json == {"to": "user@example.com"}
    assert job.priority == 5
    assert job.max_retries == 2
    assert job.timeout_seconds == 120
    assert job.idempotency_key == "order-123"
    assert job.next_run_at == datetime(2026, 6, 1, 15, 30, 0, tzinfo=UTC)
    assert job.status == JobStatus.QUEUED.value

    body = response.json()
    assert body["status"] == "queued"
    assert body["run_at"] == run_at


async def test_create_job_defaults_queue_when_omitted(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    response = await http_client.post(
        "/jobs",
        json={"job_type": "send_email"},
    )

    assert response.status_code == 201
    assert response.json()["queue"] == "default"
    assert fake_session.added[0].queue == "default"


async def test_create_job_rejects_invalid_body(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, _fake_session = client_with_session
    response = await http_client.post(
        "/jobs",
        json={"queue": "default"},
    )

    assert response.status_code == 422


async def test_get_job_returns_200_with_full_status_detail(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.jobs[1] = _make_job(last_error="timeout")

    response = await http_client.get("/jobs/1")

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == 1
    assert body["queue"] == "emails"
    assert body["job_type"] == "send_email"
    assert body["payload"] == {"to": "user@example.com"}
    assert body["status"] == "running"
    assert body["priority"] == 5
    assert body["attempts"] == 2
    assert body["max_retries"] == 3
    assert body["run_at"] == "2026-06-01T15:30:00Z"
    assert body["timeout_seconds"] == 120
    assert body["idempotency_key"] == "order-123"
    assert body["created_at"] == "2026-01-01T12:00:00Z"
    assert body["updated_at"] == "2026-01-01T12:00:00Z"
    assert body["completed_at"] is None
    assert body["failed_at"] is None
    assert body["last_error"] == "timeout"


async def test_get_job_returns_404_when_not_found(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, _fake_session = client_with_session

    response = await http_client.get("/jobs/999")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}


async def test_get_job_rejects_invalid_job_id(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, _fake_session = client_with_session

    response = await http_client.get("/jobs/not-a-number")

    assert response.status_code == 422


@pytest.mark.parametrize("job_id", [0, -1])
async def test_get_job_returns_404_for_nonexistent_boundary_job_ids(
    client_with_session: tuple[AsyncClient, FakeSession],
    job_id: int,
) -> None:
    http_client, _fake_session = client_with_session

    response = await http_client.get(f"/jobs/{job_id}")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}


async def test_get_job_does_not_expose_internal_lease_fields(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.jobs[1] = _make_job()

    response = await http_client.get("/jobs/1")

    assert response.status_code == 200
    body = response.json()
    assert "locked_by" not in body
    assert "locked_at" not in body
    assert "lease_expires_at" not in body


async def test_list_jobs_returns_200_with_paginated_envelope(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.list_jobs_rows = [
        _make_job(job_id=1, idempotency_key=None),
        _make_job(job_id=2, idempotency_key=None),
    ]
    fake_session.list_jobs_total = 2

    response = await http_client.get("/jobs")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["limit"] == DEFAULT_LIMIT
    assert body["offset"] == 0
    assert len(body["jobs"]) == 2
    assert body["jobs"][0]["job_id"] == 1
    assert body["jobs"][1]["job_id"] == 2


async def test_list_jobs_does_not_expose_internal_lease_fields(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.list_jobs_rows = [_make_job()]
    fake_session.list_jobs_total = 1

    response = await http_client.get("/jobs")

    assert response.status_code == 200
    job = response.json()["jobs"][0]
    assert "locked_by" not in job
    assert "locked_at" not in job
    assert "lease_expires_at" not in job


@pytest.mark.parametrize(
    ("query", "expected_status"),
    [
        ("status=not-a-status", 422),
        ("queue=", 422),
        ("job_type=", 422),
        ("limit=0", 422),
        (f"limit={MAX_LIMIT + 1}", 422),
        ("offset=-1", 422),
    ],
)
async def test_list_jobs_rejects_invalid_query_params(
    client_with_session: tuple[AsyncClient, FakeSession],
    query: str,
    expected_status: int,
) -> None:
    http_client, _fake_session = client_with_session

    response = await http_client.get(f"/jobs?{query}")

    assert response.status_code == expected_status


@pytest.mark.parametrize(
    "job_status",
    [JobStatus.QUEUED.value, JobStatus.SCHEDULED.value],
)
async def test_cancel_job_returns_200_for_cancellable_statuses(
    client_with_session: tuple[AsyncClient, FakeSession],
    job_status: str,
) -> None:
    http_client, fake_session = client_with_session
    cancelled_job = _make_job(job_id=1, job_status=JobStatus.CANCELLED.value)
    fake_session.update_returning_rows = [cancelled_job]

    response = await http_client.post("/jobs/1/cancel")

    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] == 1
    assert body["status"] == "cancelled"
    assert fake_session.committed is True


async def test_cancel_job_returns_404_when_not_found(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.update_returning_rows = []

    response = await http_client.post("/jobs/999/cancel")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}
    assert fake_session.committed is False


@pytest.mark.parametrize(
    "job_status",
    [
        JobStatus.RUNNING.value,
        JobStatus.SUCCEEDED.value,
        JobStatus.FAILED.value,
        JobStatus.RETRYING.value,
        JobStatus.DEAD_LETTER.value,
        JobStatus.CANCELLED.value,
    ],
)
async def test_cancel_job_returns_409_for_non_cancellable_statuses(
    client_with_session: tuple[AsyncClient, FakeSession],
    job_status: str,
) -> None:
    http_client, fake_session = client_with_session
    fake_session.update_returning_rows = []
    fake_session.jobs[1] = _make_job(job_id=1, job_status=job_status)

    response = await http_client.post("/jobs/1/cancel")

    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Job cannot be cancelled from status '{job_status}'"
    }
    assert fake_session.committed is False


async def test_cancel_job_rejects_invalid_job_id(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, _fake_session = client_with_session

    response = await http_client.post("/jobs/not-a-number/cancel")

    assert response.status_code == 422
