import asyncio
import sqlite3
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
import respx

from app.domain.payment import PaymentStatus
from app.infrastructure.gateway import HttpGateway
from app.services.errors import GatewayRejectedError
from emulator.main import EmulatorSettings, create_app
from tests.factories import payment


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "gateway.db"


@pytest.fixture
async def emulator(db_path: Path) -> AsyncIterator[httpx.AsyncClient]:
    async with run_emulator(db_path) as client:
        yield client


@asynccontextmanager
async def run_emulator(db_path: Path) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(EmulatorSettings(db_path=db_path, min_delay=0.2, max_delay=0.2))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client,
    ):
        yield client


async def charge(client: httpx.AsyncClient, key: str, amount: str = "10.00") -> httpx.Response:
    body = {"amount": amount, "currency": "USD"}
    return await client.post("/charges", json=body, headers={"Idempotency-Key": key})


def count_charges(db_path: Path, key: str) -> int:
    with sqlite3.connect(db_path) as db:
        query = "SELECT count(*) FROM charges WHERE idempotency_key = ?"
        count: int = db.execute(query, (key,)).fetchone()[0]
    return count


async def test_same_key_same_outcome(emulator: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch):
    key = str(uuid.uuid4())
    monkeypatch.setattr("random.random", lambda: 0.0)
    first = await charge(emulator, key)
    monkeypatch.setattr("random.random", lambda: 0.99)
    second = await charge(emulator, key)
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json() == {"status": "succeeded"}


async def test_key_reused_for_other_charge(emulator: httpx.AsyncClient):
    key = str(uuid.uuid4())
    await charge(emulator, key)
    assert (await charge(emulator, key, amount="10.0")).status_code == 200
    response = await charge(emulator, key, amount="11.00")
    assert response.status_code == 409


async def test_retry_after_timeout_charges_once(emulator: httpx.AsyncClient, db_path: Path):
    key = str(uuid.uuid4())
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await charge(emulator, key)
    response = await charge(emulator, key)
    assert response.status_code == 200
    assert count_charges(db_path, key) == 1


async def test_http_gateway(emulator: httpx.AsyncClient):
    pending = payment(status=PaymentStatus.PENDING)
    status = await HttpGateway(emulator).charge(pending)
    assert status in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
    assert await HttpGateway(emulator).charge(pending) == status


@respx.mock
async def test_http_gateway_rejected():
    route = respx.post("http://gw/charges")
    gateway = HttpGateway(httpx.AsyncClient(base_url="http://gw"))
    for code in (402, 422):
        route.respond(code)
        with pytest.raises(GatewayRejectedError, match=f"HTTP {code}"):
            await gateway.charge(payment())
    for code in (400, 401, 409, 429, 503):
        route.respond(code)
        with pytest.raises(httpx.HTTPStatusError):
            await gateway.charge(payment())
