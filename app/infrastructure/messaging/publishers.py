from faststream.rabbit import RabbitBroker, RabbitQueue

from app.domain.payment import PaymentCreated, PaymentDeadLettered
from app.infrastructure.messaging.broker import (
    PAYMENTS_NEW,
    dead_letter_exchange,
    new_payments_queue,
    payments_exchange,
)
from app.services.interfaces import OutboxMessage

PUBLISH_TIMEOUT = 10


class RabbitPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self.broker = broker

    async def publish(self, message: OutboxMessage) -> None:
        if message.event_type == PaymentDeadLettered.event_type:
            await self.dead_letter(message)
            return
        await self.broker.publish(
            message.payload,
            exchange=payments_exchange,
            routing_key=PAYMENTS_NEW,
            message_id=str(message.id),
            message_type=message.event_type,
            persist=True,
            timeout=PUBLISH_TIMEOUT,
        )

    async def dead_letter(self, message: OutboxMessage) -> None:
        payload = message.payload
        await self.broker.publish(
            {"payment_id": payload["payment_id"]},
            exchange=dead_letter_exchange,
            message_id=str(message.id),
            message_type=PaymentCreated.event_type,
            headers={
                "x-stage": payload["stage"],
                "x-attempts": payload["attempts"],
                "x-error": payload["error"],
                "x-gave-up-at": payload["gave_up_at"],
            },
            persist=True,
            timeout=PUBLISH_TIMEOUT,
        )


class RabbitPaymentQueue:
    def __init__(self, broker: RabbitBroker, queue: RabbitQueue = new_payments_queue) -> None:
        self.broker = broker
        self.queue = queue

    async def ready_count(self) -> int:
        declared = await self.broker.declare_queue(self.queue)
        # faststream кеширует счетчики, переобъявляем
        state = await declared.declare(timeout=PUBLISH_TIMEOUT)
        return state.message_count or 0
