from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from api.deps import get_db
from api.main import app
from core.models import JobStatus
from core.schemas import DEFAULT_LIMIT, MAX_LIMIT, QUEUE_NAME_MAX_LENGTH
from tests.queue_stats_helpers import zero_status_counts


@dataclass
class FakeQueueRow:
    queue: str
    job_count: int


@dataclass
class FakeOldestJobRow:
    id: int
    job_type: str
    status: str
    created_at: datetime
    next_run_at: datetime


class FakeResult:
    def __init__(
        self,
        *,
        scalar: int | None = None,
        rows: list[FakeQueueRow] | None = None,
        status_count_rows: list[tuple[str, int]] | None = None,
        oldest_job_row: FakeOldestJobRow | None = None,
    ) -> None:
        self._scalar = scalar
        self._rows = rows or []
        self._status_count_rows = status_count_rows
        self._oldest_job_row = oldest_job_row

    def scalar_one(self) -> int:
        assert self._scalar is not None
        return self._scalar

    def all(self) -> list[FakeQueueRow] | list[tuple[str, int]]:
        if self._status_count_rows is not None:
            return self._status_count_rows
        return self._rows

    def one_or_none(self) -> FakeQueueRow | FakeOldestJobRow | None:
        if self._oldest_job_row is not None:
            return self._oldest_job_row
        if not self._rows:
            return None
        return self._rows[0]


class FakeSession:
    def __init__(self) -> None:
        self.list_queues_rows: list[FakeQueueRow] = []
        self.list_queues_total = 0
        self.get_queue_mode = False
        self.get_queue_row: FakeQueueRow | None = None
        self.stats_mode = False
        self.stats_status_rows: list[tuple[str, int]] = []
        self.stats_oldest_job_row: FakeOldestJobRow | None = None
        self.execute_call_count = 0

    async def execute(self, statement: Any) -> FakeResult:
        self.execute_call_count += 1
        if self.stats_mode:
            if self.execute_call_count == 1:
                return FakeResult(status_count_rows=self.stats_status_rows)
            return FakeResult(oldest_job_row=self.stats_oldest_job_row)
        if self.get_queue_mode:
            rows = [self.get_queue_row] if self.get_queue_row is not None else []
            return FakeResult(rows=rows)
        if self.execute_call_count == 1:
            return FakeResult(scalar=self.list_queues_total)
        return FakeResult(rows=self.list_queues_rows)


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


async def test_list_queues_returns_200_with_paginated_envelope(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.list_queues_rows = [
        FakeQueueRow(queue="emails", job_count=3),
        FakeQueueRow(queue="reports", job_count=2),
    ]
    fake_session.list_queues_total = 2

    response = await http_client.get("/queues")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["limit"] == DEFAULT_LIMIT
    assert body["offset"] == 0
    assert body["queues"] == [
        {"queue": "emails", "job_count": 3},
        {"queue": "reports", "job_count": 2},
    ]


async def test_list_queues_empty(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.list_queues_total = 0

    response = await http_client.get("/queues")

    assert response.status_code == 200
    body = response.json()
    assert body["queues"] == []
    assert body["total"] == 0


@pytest.mark.parametrize(
    "query",
    ["limit=0", f"limit={MAX_LIMIT + 1}", "offset=-1"],
)
async def test_list_queues_rejects_invalid_pagination(
    client_with_session: tuple[AsyncClient, FakeSession],
    query: str,
) -> None:
    http_client, _fake_session = client_with_session

    response = await http_client.get(f"/queues?{query}")

    assert response.status_code == 422


async def test_get_queue_returns_200(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.get_queue_mode = True
    fake_session.get_queue_row = FakeQueueRow(queue="emails", job_count=7)

    response = await http_client.get("/queues/emails")

    assert response.status_code == 200
    assert response.json() == {"queue": "emails", "job_count": 7}


async def test_get_queue_not_found(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.get_queue_mode = True
    fake_session.get_queue_row = None

    response = await http_client.get("/queues/missing")

    assert response.status_code == 404
    assert response.json() == {"detail": "Queue not found"}


@pytest.mark.parametrize(
    "queue_name",
    [
        "Bad_Name",
        "a" * (QUEUE_NAME_MAX_LENGTH + 1),
        "-leading",
    ],
)
async def test_get_queue_rejects_invalid_queue_name(
    client_with_session: tuple[AsyncClient, FakeSession],
    queue_name: str,
) -> None:
    http_client, _fake_session = client_with_session

    response = await http_client.get(f"/queues/{queue_name}")

    assert response.status_code == 422


async def test_get_queue_stats_returns_200_with_expected_shape(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    created = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    run_at = datetime(2026, 1, 1, 12, 5, 0, tzinfo=UTC)
    fake_session.stats_mode = True
    fake_session.stats_status_rows = [
        (JobStatus.QUEUED.value, 2),
        (JobStatus.RUNNING.value, 1),
    ]
    fake_session.stats_oldest_job_row = FakeOldestJobRow(
        id=10,
        job_type="send_email",
        status=JobStatus.QUEUED.value,
        created_at=created,
        next_run_at=run_at,
    )

    response = await http_client.get("/queues/emails/stats")

    assert response.status_code == 200
    expected_counts = zero_status_counts()
    expected_counts[JobStatus.QUEUED.value] = 2
    expected_counts[JobStatus.RUNNING.value] = 1
    body = response.json()
    assert body == {
        "queue": "emails",
        "depth": 2,
        "status_counts": expected_counts,
        "oldest_job": {
            "job_id": 10,
            "job_type": "send_email",
            "status": "queued",
            "created_at": created.isoformat().replace("+00:00", "Z"),
            "run_at": run_at.isoformat().replace("+00:00", "Z"),
        },
    }
    assert fake_session.execute_call_count == 2


async def test_get_queue_stats_not_found(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.stats_mode = True
    fake_session.stats_status_rows = []

    response = await http_client.get("/queues/missing/stats")

    assert response.status_code == 404
    assert response.json() == {"detail": "Queue not found"}
    assert fake_session.execute_call_count == 1


async def test_get_queue_stats_terminal_only_has_null_oldest_job(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    fake_session.stats_mode = True
    fake_session.stats_status_rows = [
        (JobStatus.SUCCEEDED.value, 1),
        (JobStatus.RUNNING.value, 1),
    ]

    response = await http_client.get("/queues/reports/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["depth"] == 0
    assert body["oldest_job"] is None
    assert fake_session.execute_call_count == 1


async def test_get_queue_stats_oldest_job_excludes_sensitive_fields(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    created = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    fake_session.stats_mode = True
    fake_session.stats_status_rows = [(JobStatus.QUEUED.value, 1)]
    fake_session.stats_oldest_job_row = FakeOldestJobRow(
        id=1,
        job_type="t",
        status=JobStatus.QUEUED.value,
        created_at=created,
        next_run_at=created,
    )

    response = await http_client.get("/queues/q/stats")

    assert response.status_code == 200
    oldest = response.json()["oldest_job"]
    assert set(oldest.keys()) == {
        "job_id",
        "job_type",
        "status",
        "created_at",
        "run_at",
    }


@pytest.mark.parametrize(
    "queue_name",
    [
        "Bad_Name",
        "a" * (QUEUE_NAME_MAX_LENGTH + 1),
        "-leading",
    ],
)
async def test_get_queue_stats_rejects_invalid_queue_name(
    client_with_session: tuple[AsyncClient, FakeSession],
    queue_name: str,
) -> None:
    http_client, _fake_session = client_with_session

    response = await http_client.get(f"/queues/{queue_name}/stats")

    assert response.status_code == 422


async def test_get_queue_stats_retrying_counts_toward_depth(
    client_with_session: tuple[AsyncClient, FakeSession],
) -> None:
    http_client, fake_session = client_with_session
    created = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    fake_session.stats_mode = True
    fake_session.stats_status_rows = [
        (JobStatus.RETRYING.value, 1),
        (JobStatus.SUCCEEDED.value, 2),
    ]
    fake_session.stats_oldest_job_row = FakeOldestJobRow(
        id=5,
        job_type="retry_me",
        status=JobStatus.RETRYING.value,
        created_at=created,
        next_run_at=created,
    )

    response = await http_client.get("/queues/work/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["depth"] == 1
    assert body["status_counts"][JobStatus.RETRYING.value] == 1
    assert body["oldest_job"]["status"] == JobStatus.RETRYING.value
    assert fake_session.execute_call_count == 2
