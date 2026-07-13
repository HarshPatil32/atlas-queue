from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient

from api.deps import get_db
from api.main import app
from core.models import Job, JobStatus


def _make_job(
    *,
    job_id: int = 1,
    job_status: str = JobStatus.RUNNING.value,
    last_error: str | None = None,
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
        idempotency_key="order-123",
        locked_by="worker-1",
        locked_at=now,
        lease_expires_at=datetime(2026, 1, 1, 12, 5, 0, tzinfo=UTC),
        last_error=last_error,
        created_at=now,
        updated_at=now,
        completed_at=None,
        failed_at=None,
    )


class FakeSession:
    def __init__(self) -> None:
        self.added: list[Job] = []
        self.jobs: dict[int, Job] = {}
        self.committed = False
        self.next_run_at_unset_before_refresh: list[bool] = []

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
