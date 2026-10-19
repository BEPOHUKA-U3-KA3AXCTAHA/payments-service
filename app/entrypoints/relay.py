import asyncio
import logging

from faststream import FastStream

from app.infrastructure.config import Settings, setup_logging
from app.infrastructure.db.engine import create_db_engine, session_factory
from app.infrastructure.db.repository import unit_of_work
from app.infrastructure.messaging.broker import create_broker, declare_topology, retry_queues
from app.infrastructure.messaging.publishers import RabbitPaymentQueue, RabbitPublisher
from app.services.outbox import OutboxRelay
from app.services.payments import requeue_stuck

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastStream:
    settings = settings or Settings()
    broker = create_broker(settings)
    engine = create_db_engine(settings, pool_size=2)
    uow = unit_of_work(session_factory(engine), settings.db_timeout)
    relay = OutboxRelay(uow, RabbitPublisher(broker))
    payment_queue = RabbitPaymentQueue(broker)
    delay_queues = list(retry_queues(settings.retry_policy()).values())
    tasks: list[asyncio.Task[None]] = []

    async def publish() -> None:
        while True:
            try:
                published = await relay.publish_batch()
            except Exception as exc:
                logger.warning("Outbox relay error: %r", exc)
                published = 0
            if published < relay.batch_size:
                await asyncio.sleep(settings.outbox_poll_interval)

    async def housekeeping() -> None:
        while True:
            try:
                await requeue_stuck(uow, payment_queue, settings.stuck_after)
                await relay.cleanup(settings.outbox_retention)
            except Exception as exc:
                logger.warning("Housekeeping error: %r", exc)
            await asyncio.sleep(settings.sweep_interval)

    async def start() -> None:
        setup_logging()
        await broker.connect()
        await declare_topology(broker, delay_queues)
        tasks.append(asyncio.create_task(publish()))
        tasks.append(asyncio.create_task(housekeeping()))

    async def stop() -> None:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await engine.dispose()

    return FastStream(
        broker,
        on_startup=[start],
        on_shutdown=[stop],
    )
