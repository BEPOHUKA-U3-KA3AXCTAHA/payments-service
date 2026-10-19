import logging

from faststream.rabbit import (
    Channel,
    ExchangeType,
    QueueType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)
from faststream.rabbit.schemas.queue import QuorumQueueArgs

from app.infrastructure.config import Settings
from app.services.retry import RetryPolicy

PAYMENTS_NEW = "payments.new"

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT)
dead_letter_exchange = RabbitExchange("payments.dlx", type=ExchangeType.FANOUT)


def quorum_queue(name: str, arguments: QuorumQueueArgs) -> RabbitQueue:
    return RabbitQueue(
        name,
        queue_type=QueueType.QUORUM,
        arguments={
            **arguments,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )


new_payments_queue = quorum_queue(
    PAYMENTS_NEW, {"x-delivery-limit": 10, "x-dead-letter-exchange": dead_letter_exchange.name}
)
# Get messages в админке возвращает сообщение через nack с requeue, это тоже доставка.
# С лимитом по умолчанию (20) сообщения из DLQ пропадут, пока их разглядываешь
dead_letter_queue = RabbitQueue(
    "payments.dlq", queue_type=QueueType.QUORUM, arguments={"x-delivery-limit": -1}
)


def retry_queues(policy: RetryPolicy) -> dict[float, RabbitQueue]:
    return {
        delay: quorum_queue(
            f"{PAYMENTS_NEW}.retry.{delay:g}s",
            {
                "x-message-ttl": round(delay * 1000),
                "x-dead-letter-exchange": payments_exchange.name,
                "x-dead-letter-routing-key": PAYMENTS_NEW,
            },
        )
        for delay in policy.delays
    }


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=settings.prefetch, on_return_raises=True),
        logger=logging.getLogger("app.broker"),
    )


async def declare_topology(broker: RabbitBroker, delay_queues: list[RabbitQueue]) -> None:
    exchange = await broker.declare_exchange(payments_exchange)
    for queue in (new_payments_queue, *delay_queues):
        q = await broker.declare_queue(queue)
        await q.bind(exchange, routing_key=queue.name)

    dlx = await broker.declare_exchange(dead_letter_exchange)
    dlq = await broker.declare_queue(dead_letter_queue)
    await dlq.bind(dlx)
