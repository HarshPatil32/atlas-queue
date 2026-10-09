from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from api.deps import get_db
from api.main import app
from core.schemas import DEFAULT_LIMIT, MAX_LIMIT, QUEUE_NAME_MAX_LENGTH


@dataclass
class FakeQueueRow:
    queue: str
    job_count: int


class FakeResult:
    def __init__(
        self,
        *,
        scalar: int | None = None,
        rows: list[FakeQueueRow] | None = None,
    ) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one(self) -> int:
        assert self._scalar is not None
        return self._scalar

    def all(self) -> list[FakeQueueRow]:
        return self._rows

    def one_or_none(self) -> FakeQueueRow | None:
        if not self._rows:
            return None
        return self._rows[0]


class FakeSession:
    def __init__(self) -> None:
        self.list_queues_rows: list[FakeQueueRow] = []
        self.list_queues_total = 0
        self.get_queue_mode = False
        self.get_queue_row: FakeQueueRow | None = None
        self.execute_call_count = 0

    async def execute(self, statement: Any) -> FakeResult:
        self.execute_call_count += 1
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
