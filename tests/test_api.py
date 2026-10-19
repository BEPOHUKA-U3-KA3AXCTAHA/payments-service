import asyncio
import uuid
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import func, select

from app.infrastructure.db.engine import Sessions
from app.infrastructure.db.models import OutboxEvent, PaymentRow


def payment_body(**changes: Any) -> dict[str, Any]:
    return {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://hook.test/ok",
    } | changes


async def create(client: AsyncClient, key: str, body: dict[str, Any] | None = None) -> Response:
    headers = {"Idempotency-Key": key}
    return await client.post("/api/v1/payments", json=body or payment_body(), headers=headers)


async def count_rows(sessions: Sessions, key: str) -> tuple[int, int]:
    async with sessions() as session:
        ids = (
            await session.scalars(select(PaymentRow.id).where(PaymentRow.idempotency_key == key))
        ).all()
        events = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.payment_id.in_(ids))
        )
    return len(ids), events or 0


@pytest.mark.parametrize("api_key", [None, "wrong-key"])
async def test_api_key_is_required(client: AsyncClient, api_key: str | None):
    client.headers.pop("X-API-Key")
    headers = {"X-API-Key": api_key} if api_key else {}
    response = await client.post("/api/v1/payments", json=payment_body(), headers=headers)
    assert response.status_code == 401
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers=headers)
    assert response.status_code == 401
    response = await client.get("/api/v1/health", headers=headers)
    assert response.status_code == 401
    response = await client.get("/api/v1/openapi.json", headers=headers)
    assert response.status_code == 401
    response = await client.get("/unknown", headers=headers)
    assert response.status_code == 401


async def test_create_and_get(client: AsyncClient):
    created = await create(client, str(uuid.uuid4()))
    assert created.status_code == 202
    assert created.json()["status"] == "pending"

    payment_id = created.json()["payment_id"]
    fetched = await client.get(f"/api/v1/payments/{payment_id}")
    assert fetched.status_code == 200
    assert fetched.json()["payment_id"] == payment_id
    assert fetched.json()["amount"] == "100.50"
    assert fetched.json()["metadata"] == {"order_id": 42}
    assert {"stage", "attempts", "gave_up_at", "last_error"} <= fetched.json().keys()
    assert fetched.json()["stage"] == "charge"


async def test_repeat_returns_same_payment(client: AsyncClient, sessions: Sessions):
    key = str(uuid.uuid4())
    first, second = await create(client, key), await create(client, key)
    assert second.status_code == 202
    assert first.json() == second.json()
    assert await count_rows(sessions, key) == (1, 1)


async def test_same_key_other_body(client: AsyncClient):
    key = str(uuid.uuid4())
    await create(client, key)
    response = await create(client, key, payment_body(amount="1.00"))
    assert response.status_code == 409


async def test_parallel_duplicates(client: AsyncClient, sessions: Sessions):
    key = str(uuid.uuid4())
    responses = await asyncio.gather(*(create(client, key) for _ in range(20)))
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await count_rows(sessions, key) == (1, 1)


@pytest.mark.parametrize(
    "body",
    [
        payment_body(amount="0"),
        payment_body(currency="GBP"),
        payment_body(webhook_url="not-a-url"),
        payment_body(metadata={"big": "x" * 20_000}),
        payment_body(webhook_url="http://10.0.0.1/x"),
        payment_body(webhook_url="http://localhost:8080/x"),
    ],
)
async def test_invalid_body(client: AsyncClient, body: dict[str, Any]):
    response = await create(client, str(uuid.uuid4()), body)
    assert response.status_code == 422


async def test_unknown_payment(client: AsyncClient):
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert response.status_code == 404
