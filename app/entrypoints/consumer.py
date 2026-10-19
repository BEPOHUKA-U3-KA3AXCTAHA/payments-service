import asyncio
import logging

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from app.domain.payment import PaymentCreated
from app.infrastructure.config import ConsumerSettings, setup_logging
from app.infrastructure.db.engine import create_db_engine, session_factory
from app.infrastructure.db.repository import unit_of_work
from app.infrastructure.gateway import HttpGateway
from app.infrastructure.messaging.broker import (
    create_broker,
    declare_topology,
    new_payments_queue,
    payments_exchange,
    retry_queues,
)
from app.infrastructure.webhooks import HttpNotifier
from app.services.errors import PaymentNotFoundError
from app.services.interfaces import Gateway
from app.services.processing import PaymentProcessor, Retry
from app.services.reconcile import Reconciler

logger = logging.getLogger(__name__)


async def move(broker: RabbitBroker, message: RabbitMessage, queue: str) -> None:
    try:
        await broker.publish(
            message.body,
            exchange=payments_exchange,
            routing_key=queue,
            message_id=message.message_id,
            content_type="application/json",
            persist=True,
        )
    except Exception:
        logger.exception("Could not move message %s", message.message_id)
        await message.nack()


def create_app(
    settings: ConsumerSettings | None = None, gateway: Gateway | None = None
) -> FastStream:
    settings = settings or ConsumerSettings()
    policy = settings.retry_policy()
    broker = create_broker(settings)
    engine = create_db_engine(settings, pool_size=2 * settings.prefetch)
    uow = unit_of_work(session_factory(engine), settings.db_timeout)
    webhooks_http = httpx.AsyncClient(timeout=settings.webhook_timeout, trust_env=False)
    gateway_http = httpx.AsyncClient(
        base_url=settings.gateway_url, timeout=settings.gateway_timeout
    )
    gateway = gateway or HttpGateway(gateway_http)
    processor = PaymentProcessor(
        uow=uow,
        gateway=gateway,
        notifier=HttpNotifier(
            webhooks_http,
            settings.webhook_timeout,
            settings.webhook_secret.get_secret_value().encode(),
            allowed_hosts=settings.webhook_allowed_hosts,
        ),
        policy=policy,
        gateway_timeout=settings.gateway_timeout,
        webhook_timeout=settings.webhook_timeout,
        lease_margin=settings.lease_margin,
    )
    reconciler = Reconciler(
        uow=uow,
        gateway=gateway,
        gateway_timeout=settings.gateway_timeout,
        lease_margin=settings.lease_margin,
        older_than=settings.reconcile_interval,
        batch_size=settings.prefetch,
    )
    delay_queues = retry_queues(policy)
    tasks: list[asyncio.Task[None]] = []

    @broker.subscriber(new_payments_queue, payments_exchange, ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def on_new_payment(event: PaymentCreated, message: RabbitMessage) -> None:
        try:
            retry = await processor.process(event.payment_id)
        except PaymentNotFoundError:
            logger.error("Payment %s not found", event.payment_id)
            raise
        except Exception as exc:
            logger.exception("Payment %s failed, will retry", event.payment_id)
            retry = Retry(policy.delays[-1], repr(exc))
        if retry is not None:
            queue = delay_queues[retry.delay].name
            logger.info("Payment %s: %s, retry via %s", event.payment_id, retry.error, queue)
            await move(broker, message, queue)

    async def reconcile() -> None:
        while True:
            await asyncio.sleep(settings.reconcile_interval)
            try:
                await reconciler.run()
            except Exception as exc:
                logger.warning("Reconcile error: %r", exc)

    async def start() -> None:
        setup_logging()
        await broker.connect()
        await declare_topology(broker, list(delay_queues.values()))
        tasks.append(asyncio.create_task(reconcile()))

    async def close() -> None:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await webhooks_http.aclose()
        await gateway_http.aclose()
        await engine.dispose()

    return FastStream(
        broker,
        on_startup=[start],
        after_shutdown=[close],
    )
