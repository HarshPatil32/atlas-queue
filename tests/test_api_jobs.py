from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient

from api.deps import get_db
from api.main import app
from core.models import Job, JobStatus


class FakeSession:
    def __init__(self) -> None:
        self.added: list[Job] = []
        self.committed = False
        self.next_run_at_unset_before_refresh: list[bool] = []

    def add(self, job: Job) -> None:
        self.added.append(job)

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
