import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from decimal import Decimal

import httpx
import pytest
import respx
from aio_pika.abc import AbstractIncomingMessage
from faststream import TestApp
from faststream.rabbit import RabbitBroker
from httpx import AsyncClient
from sqlalchemy import func, select

from app.domain.payment import Currency, Payment, PaymentDeadLettered, PaymentStatus
from app.entrypoints import consumer, relay
from app.infrastructure.config import ConsumerSettings
from app.infrastructure.db.engine import Sessions
from app.infrastructure.db.models import OutboxEvent, PaymentRow
from app.infrastructure.db.repository import SqlAttempts, unit_of_work
from app.infrastructure.messaging.broker import (
    PAYMENTS_NEW,
    create_broker,
    dead_letter_queue,
    declare_topology,
    payments_exchange,
    retry_queues,
)
from app.infrastructure.webhooks import HttpNotifier
from app.services.dead_letters import revive
from app.services.errors import GatewayRejectedError
from app.services.interfaces import UnitOfWork
from app.services.payments import requeue_stuck
from app.services.processing import PaymentProcessor
from app.services.reconcile import Reconciler


class FakeGateway:
    def __init__(self) -> None:
        self.charged: list[uuid.UUID] = []
        self.delay = 0.0
        self.unreachable = False
        self.rejected = False
        self.undecided = False

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.charged.append(payment.id)
        await asyncio.sleep(self.delay)
        if self.unreachable:
            raise ConnectionError
        if self.rejected:
            raise GatewayRejectedError("HTTP 422")
        return PaymentStatus.PENDING if self.undecided else PaymentStatus.SUCCEEDED


class FakePaymentQueue:
    def __init__(self, ready: int) -> None:
        self.ready = ready

    async def ready_count(self) -> int:
        return self.ready


@pytest.fixture(scope="session")
def gateway() -> FakeGateway:
    return FakeGateway()


@pytest.fixture(scope="session")
async def hooks(
    settings: ConsumerSettings, gateway: FakeGateway
) -> AsyncIterator[respx.MockRouter]:
    with respx.mock(base_url="https://hook.test", assert_all_called=False) as router:
        router.post("/ok", name="ok").respond(200)
        router.post("/fail", name="fail").respond(500)
        router.post("/gone", name="gone").respond(410)
        router.post("/missing", name="missing").respond(404)
        async with (
            TestApp(relay.create_app(settings)),
            TestApp(consumer.create_app(settings, gateway)),
        ):
            yield router


@pytest.fixture(scope="session")
async def broker(settings: ConsumerSettings) -> AsyncIterator[RabbitBroker]:
    async with create_broker(settings) as broker:
        await declare_topology(broker, list(retry_queues(settings.retry_policy()).values()))
        yield broker


async def create_payment(client: AsyncClient, webhook_url: str) -> str:
    body = {"amount": "10.00", "currency": "USD", "description": "Test", "webhook_url": webhook_url}
    response = await client.post(
        "/api/v1/payments", json=body, headers={"Idempotency-Key": str(uuid.uuid4())}
    )
    return response.json()["payment_id"]


async def publish_event(broker: RabbitBroker, payment_id: str) -> None:
    await broker.publish(
        {"payment_id": payment_id}, exchange=payments_exchange, routing_key=PAYMENTS_NEW
    )


def calls_for(route: respx.Route, payment_id: str) -> list[httpx.Request]:
    return [
        c.request for c in route.calls if json.loads(c.request.content)["payment_id"] == payment_id
    ]


async def wait_until(condition: Callable[[], object]) -> None:
    for _ in range(200):
        if condition():
            return
        await asyncio.sleep(0.05)
    raise TimeoutError


async def get_payment(sessions: Sessions, payment_id: str) -> PaymentRow:
    async with sessions() as session:
        return await session.get_one(PaymentRow, uuid.UUID(payment_id))


async def dead_letter(broker: RabbitBroker, payment_id: str) -> AbstractIncomingMessage:
    async with asyncio.timeout(10):
        while not (messages := await dead_letters(broker, payment_id)):
            await asyncio.sleep(0.05)
    return messages[0]


async def take_dead_letters(broker: RabbitBroker) -> dict[str, list[AbstractIncomingMessage]]:
    queue = await broker.declare_queue(dead_letter_queue)
    found: dict[str, list[AbstractIncomingMessage]] = {}
    while message := await queue.get(fail=False):
        await message.ack()
        found.setdefault(json.loads(message.body)["payment_id"], []).append(message)
    return found


async def dead_letters(broker: RabbitBroker, payment_id: str) -> list[AbstractIncomingMessage]:
    return (await take_dead_letters(broker)).get(payment_id, [])


async def insert_payment(sessions: Sessions, **columns: object) -> str:
    async with sessions.begin() as session:
        row = PaymentRow(
            amount=Decimal("10.00"),
            currency=Currency.USD,
            description="Test",
            meta={},
            idempotency_key=str(uuid.uuid4()),
            request_hash="test",
            webhook_url="https://hook.test/ok",
            **columns,
        )
        session.add(row)
    return str(row.id)


async def test_happy_path(client: AsyncClient, hooks: respx.MockRouter, sessions: Sessions):
    statuses = []

    async def receiver(request):
        payment = await get_payment(sessions, json.loads(request.content)["payment_id"])
        statuses.append(payment.status)
        return httpx.Response(200)

    route = hooks.post("/check").mock(side_effect=receiver)
    payment_id = await create_payment(client, "https://hook.test/check")
    for _ in range(200):
        payment = await get_payment(sessions, payment_id)
        if payment.notified_at:
            break
        await asyncio.sleep(0.05)
    assert statuses == [PaymentStatus.SUCCEEDED]
    requests = calls_for(route, payment_id)
    assert len(requests) == 1
    assert requests[0].headers["X-Webhook-Id"] == payment_id
    assert json.loads(requests[0].content)["status"] == "succeeded"

    async with sessions() as session:
        event = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.payment_id == uuid.UUID(payment_id))
        )
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.last_error is None
    assert event is not None
    assert event.published_at is not None


async def test_webhook_500_to_dlq(
    client: AsyncClient, hooks: respx.MockRouter, broker: RabbitBroker, sessions: Sessions
):
    payment_id = await create_payment(client, "https://hook.test/fail")
    message = await dead_letter(broker, payment_id)
    assert message.headers["x-stage"] == "webhook"
    assert message.headers["x-attempts"] == 3
    assert message.headers["x-error"] == "HTTP 500"
    assert len(calls_for(hooks["fail"], payment_id)) == 3
    payment = await get_payment(sessions, payment_id)
    assert payment.notified_at is None
    assert payment.gave_up_at is not None
    assert payment.last_error == "HTTP 500"
    assert payment.attempts == 3
    assert json.loads(message.body) == {"payment_id": payment_id}
    assert message.type == "payment.created"
    assert datetime.fromisoformat(message.headers["x-gave-up-at"]) == payment.gave_up_at
    async with sessions() as session:
        event_id = await session.scalar(
            select(OutboxEvent.id).where(
                OutboxEvent.payment_id == payment.id,
                OutboxEvent.event_type == PaymentDeadLettered.event_type,
            )
        )
    assert message.message_id == str(event_id)


async def test_unknown_payment_to_dlq(hooks: respx.MockRouter, broker: RabbitBroker):
    payment_id = str(uuid.uuid4())
    await publish_event(broker, payment_id)
    message = await dead_letter(broker, payment_id)
    assert message.headers["x-death"][0]["reason"] == "rejected"
    assert "x-gave-up-at" not in message.headers


async def test_double_delivery(
    client: AsyncClient,
    hooks: respx.MockRouter,
    broker: RabbitBroker,
    gateway: FakeGateway,
    sessions: Sessions,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(gateway, "delay", 0.3)
    handled = 0
    process = PaymentProcessor.process

    async def counting_process(self, processed_id):
        nonlocal handled
        outcome = await process(self, processed_id)
        if str(processed_id) == payment_id and outcome is None:
            handled += 1
        return outcome

    monkeypatch.setattr(PaymentProcessor, "process", counting_process)
    payment_id = await create_payment(client, "https://hook.test/ok")
    await publish_event(broker, payment_id)
    await publish_event(broker, payment_id)
    await wait_until(lambda: handled == 3)
    assert gateway.charged.count(uuid.UUID(payment_id)) == 1
    assert len(calls_for(hooks["ok"], payment_id)) == 1
    payment = await get_payment(sessions, payment_id)
    assert payment.attempts == 1


async def test_gateway_rejected(
    client: AsyncClient,
    hooks: respx.MockRouter,
    gateway: FakeGateway,
    sessions: Sessions,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(gateway, "rejected", True)
    payment_id = await create_payment(client, "https://hook.test/ok")
    await wait_until(lambda: calls_for(hooks["ok"], payment_id))
    [request] = calls_for(hooks["ok"], payment_id)
    assert json.loads(request.content)["status"] == "failed"
    payment = await get_payment(sessions, payment_id)
    assert payment.status == PaymentStatus.FAILED
    assert payment.failure_reason == "gateway rejected: HTTP 422"
    assert gateway.charged.count(payment.id) == 1


async def test_gateway_down(
    client: AsyncClient,
    hooks: respx.MockRouter,
    broker: RabbitBroker,
    gateway: FakeGateway,
    sessions: Sessions,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(gateway, "unreachable", True)
    payment_id = await create_payment(client, "https://hook.test/ok")
    message = await dead_letter(broker, payment_id)
    assert message.headers["x-stage"] == "charge"
    assert message.headers["x-attempts"] == 3
    assert message.headers["x-error"] == "gateway unavailable"
    assert gateway.charged.count(uuid.UUID(payment_id)) == 3
    payment = await get_payment(sessions, payment_id)
    assert payment.status == PaymentStatus.PENDING
    assert calls_for(hooks["ok"], payment_id) == []

    monkeypatch.setattr(gateway, "unreachable", False)
    assert await revive(unit_of_work(sessions), [payment.id]) == [payment.id]
    async with asyncio.timeout(10):
        while not (payment := await get_payment(sessions, payment_id)).notified_at:
            await asyncio.sleep(0.05)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert len(calls_for(hooks["ok"], payment_id)) == 1
    assert payment.attempts == 1


def lost_commit_reply(
    sessions: Sessions, monkeypatch: pytest.MonkeyPatch, payment_id: uuid.UUID
) -> UnitOfWork:
    saved = False
    set_result = SqlAttempts.set_result

    async def saving(self, saved_id, *args):
        nonlocal saved
        saved = saved or saved_id == payment_id
        return await set_result(self, saved_id, *args)

    monkeypatch.setattr(SqlAttempts, "set_result", saving)
    uow = unit_of_work(sessions)

    @asynccontextmanager
    async def transaction():
        nonlocal saved
        async with uow() as tx:
            yield tx
        if saved:
            saved = False
            raise TimeoutError

    return transaction


@asynccontextmanager
async def processor(
    uow: UnitOfWork, gateway: FakeGateway, settings: ConsumerSettings
) -> AsyncIterator[PaymentProcessor]:
    async with httpx.AsyncClient() as http:
        yield PaymentProcessor(
            uow=uow,
            gateway=gateway,
            notifier=HttpNotifier(http, 0.5, b"secret", frozenset({"hook.test"})),
            policy=settings.retry_policy(),
            gateway_timeout=0.5,
            webhook_timeout=0.5,
            lease_margin=10,
        )


@pytest.mark.parametrize("charge_attempts", [0, 2])
async def test_commit_timeout_after_charge(
    hooks: respx.MockRouter,
    gateway: FakeGateway,
    sessions: Sessions,
    settings: ConsumerSettings,
    monkeypatch: pytest.MonkeyPatch,
    charge_attempts: int,
):
    payment_id = await insert_payment(sessions, attempts=charge_attempts)
    target = uuid.UUID(payment_id)
    uow = lost_commit_reply(sessions, monkeypatch, target)
    async with processor(uow, gateway, settings) as payments:
        retry = await payments.process(target)
        assert retry is not None
        payment = await get_payment(sessions, payment_id)
        assert payment.status == PaymentStatus.SUCCEEDED
        assert payment.notified_at is None
        assert payment.attempts == 1
        await asyncio.sleep(retry.delay + 0.2)
        assert await payments.process(target) is None
    payment = await get_payment(sessions, payment_id)
    assert payment.notified_at is not None
    assert payment.attempts == 2
    assert gateway.charged.count(target) == 1
    assert len(calls_for(hooks["ok"], payment_id)) == 1


async def test_stuck_payment_requeued(hooks: respx.MockRouter, sessions: Sessions):
    payment_id = await insert_payment(sessions, next_attempt_at=func.now() - timedelta(hours=1))
    await insert_payment(
        sessions, next_attempt_at=func.now() - timedelta(seconds=5), lock_token=uuid.uuid4()
    )
    uow = unit_of_work(sessions)
    assert await requeue_stuck(uow, FakePaymentQueue(ready=10**6), stuck_after=600) == 0
    assert await requeue_stuck(uow, FakePaymentQueue(ready=0), stuck_after=600) == 1
    assert await requeue_stuck(uow, FakePaymentQueue(ready=0), stuck_after=600) == 0
    await wait_until(lambda: calls_for(hooks["ok"], payment_id))
    assert len(calls_for(hooks["ok"], payment_id)) == 1


def reconciler(sessions: Sessions, gateway: FakeGateway) -> Reconciler:
    return Reconciler(
        uow=unit_of_work(sessions),
        gateway=gateway,
        gateway_timeout=0.5,
        lease_margin=10,
        older_than=3600,
        batch_size=10,
    )


async def abandoned_payment(sessions: Sessions) -> str:
    two_hours_ago = func.now() - timedelta(hours=2)
    return await insert_payment(
        sessions,
        attempts=3,
        last_error="gateway unavailable",
        gave_up_at=two_hours_ago,
        next_attempt_at=two_hours_ago,
    )


async def test_reconciler_resolves_abandoned(
    hooks: respx.MockRouter, gateway: FakeGateway, sessions: Sessions
):
    payment_id = await abandoned_payment(sessions)
    assert await reconciler(sessions, gateway).run() == 1
    assert await reconciler(sessions, gateway).run() == 0
    async with asyncio.timeout(10):
        while not (payment := await get_payment(sessions, payment_id)).notified_at:
            await asyncio.sleep(0.05)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.gave_up_at is None
    assert gateway.charged.count(payment.id) == 1
    assert len(calls_for(hooks["ok"], payment_id)) == 1
    assert payment.attempts == 1


async def test_redrive_skips_locked(sessions: Sessions):
    payment_id = await insert_payment(
        sessions,
        status=PaymentStatus.SUCCEEDED,
        processed_at=func.now(),
        attempts=3,
        gave_up_at=func.now() - timedelta(hours=1),
        next_attempt_at=func.now() + timedelta(seconds=60),
        lock_token=uuid.uuid4(),
    )
    payment = await get_payment(sessions, payment_id)
    assert await revive(unit_of_work(sessions), [payment.id]) == []
    payment = await get_payment(sessions, payment_id)
    assert payment.gave_up_at is not None
    assert payment.attempts == 3
