from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from api.deps import get_db
from api.main import app
from core.models import Job, JobStatus
from core.schemas import DEFAULT_LIMIT, MAX_LIMIT


def _make_dead_letter_job(
    *,
    job_id: int = 1,
    queue: str = "emails",
    job_type: str = "send_email",
    job_status: str = JobStatus.DEAD_LETTER.value,
    last_error: str | None = "max retries exceeded",
) -> Job:
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    return Job(
        id=job_id,
        queue=queue,
        job_type=job_type,
        payload_json={"to": "user@example.com"},
        status=job_status,
        priority=5,
        attempts=3,
        max_retries=3,
        next_run_at=now,
        timeout_seconds=120,
        idempotency_key=None,
        last_error=last_error,
        created_at=now,
        updated_at=now,
        failed_at=now,
        dead_lettered_at=now,
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

    def scalar_one_or_none(self) -> int | None:
        return self._scalar

    def scalars(self) -> "FakeResult":
        return self

    def all(self) -> list[Job]:
        return self._rows


class FakeSession:
    def __init__(self) -> None:
        self.list_jobs_rows: list[Job] = []
        self.list_jobs_total = 0
        self.execute_call_count = 0
        self.jobs: dict[int, Job] = {}
        self.delete_returning_id: int | None = None
        self.committed = False

    async def get(self, model: type, job_id: int) -> Job | None:
        return self.jobs.get(job_id)

    async def commit(self) -> None:
        self.committed = True

    async def execute(self, statement: Any) -> FakeResult:
        if getattr(statement, "is_delete", False):
            return FakeResult(scalar=self.delete_returning_id)

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


async def test_list_dead_letter_jobs_returns_200_with_paginated_envelope(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.list_jobs_rows = [
        _make_dead_letter_job(job_id=1),
        _make_dead_letter_job(job_id=2, last_error="lease expired"),
    ]
    fake_session.list_jobs_total = 2

    response = await http_client.get("/dead-letter")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["limit"] == DEFAULT_LIMIT
    assert body["offset"] == 0
    assert len(body["jobs"]) == 2
    assert body["jobs"][0]["job_id"] == 1
    assert body["jobs"][1]["job_id"] == 2


async def test_list_dead_letter_jobs_includes_failure_reason_and_dead_lettered_at(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.list_jobs_rows = [_make_dead_letter_job(last_error="handler crashed")]
    fake_session.list_jobs_total = 1

    response = await http_client.get("/dead-letter")

    assert response.status_code == 200
    job = response.json()["jobs"][0]
    assert job["status"] == "dead_letter"
    assert job["last_error"] == "handler crashed"
    assert job["dead_lettered_at"] == "2026-01-01T12:00:00Z"


async def test_list_dead_letter_jobs_serializes_null_last_error(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.list_jobs_rows = [_make_dead_letter_job(last_error=None)]
    fake_session.list_jobs_total = 1

    response = await http_client.get("/dead-letter")

    assert response.status_code == 200
    assert response.json()["jobs"][0]["last_error"] is None


async def test_list_dead_letter_jobs_does_not_expose_internal_lease_fields(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.list_jobs_rows = [_make_dead_letter_job()]
    fake_session.list_jobs_total = 1

    response = await http_client.get("/dead-letter")

    assert response.status_code == 200
    job = response.json()["jobs"][0]
    assert "locked_by" not in job
    assert "locked_at" not in job
    assert "lease_expires_at" not in job


@pytest.mark.parametrize(
    "query",
    [
        "queue=reports",
        "job_type=send_email",
        "limit=10&offset=5",
        "queue=reports&job_type=send_email&limit=10&offset=5",
    ],
)
async def test_list_dead_letter_jobs_accepts_filter_and_pagination_params(
    client_with_session: tuple[AsyncClient, FakeSession],
    query: str,
) -> None:
    http_client, fake_session = client_with_session
    fake_session.list_jobs_rows = []
    fake_session.list_jobs_total = 0

    response = await http_client.get(f"/dead-letter?{query}")

    assert response.status_code == 200


@pytest.mark.parametrize(
    ("query", "expected_status"),
    [
        ("queue=", 422),
        ("job_type=", 422),
        ("limit=0", 422),
        (f"limit={MAX_LIMIT + 1}", 422),
        ("offset=-1", 422),
    ],
)
async def test_list_dead_letter_jobs_rejects_invalid_query_params(
    client_with_session: tuple[AsyncClient, FakeSession],
    query: str,
    expected_status: int,
) -> None:
    http_client, _fake_session = client_with_session

    response = await http_client.get(f"/dead-letter?{query}")

    assert response.status_code == expected_status


async def test_delete_dead_letter_job_returns_204(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.delete_returning_id = 1

    response = await http_client.delete("/dead-letter/1")

    assert response.status_code == 204
    assert response.content == b""
    assert fake_session.committed is True


async def test_delete_dead_letter_job_returns_404_when_job_missing(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.delete_returning_id = None

    response = await http_client.delete("/dead-letter/999")

    assert response.status_code == 404
    assert response.json() == {"detail": "Job not found"}
    assert fake_session.committed is False


async def test_delete_dead_letter_job_returns_409_when_not_dead_letter(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.delete_returning_id = None
    fake_session.jobs[1] = _make_dead_letter_job(
        job_id=1,
        job_status=JobStatus.QUEUED.value,
    )

    response = await http_client.delete("/dead-letter/1")

    assert response.status_code == 409
    assert response.json() == {"detail": "Job is not in dead_letter status"}
    assert fake_session.committed is False


async def test_delete_dead_letter_job_rejects_invalid_job_id(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, _fake_session = client_with_session

    response = await http_client.delete("/dead-letter/not-a-number")

    assert response.status_code == 422
