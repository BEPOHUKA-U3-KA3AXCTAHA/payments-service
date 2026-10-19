import asyncio
import logging
from dataclasses import dataclass

from app.services.interfaces import Publisher, UnitOfWork

logger = logging.getLogger(__name__)


@dataclass
class OutboxRelay:
    uow: UnitOfWork
    publisher: Publisher
    batch_size: int = 100
    lease: float = 60

    async def publish_batch(self) -> int:
        async with self.uow() as tx:
            messages = await tx.outbox.take(self.batch_size, self.lease)
        if not messages:
            return 0

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.lease / 2
        published: list[int] = []
        for message in messages:
            if loop.time() > deadline:
                break
            try:
                await self.publisher.publish(message)
            except Exception as exc:
                logger.warning("Outbox publish failed on event %d: %r", message.id, exc)
                break
            published.append(message.id)

        async with self.uow() as tx:
            await tx.outbox.mark_published(published)
            await tx.outbox.release([m.id for m in messages if m.id not in published])
        return len(published)

    async def cleanup(self, retention: float) -> None:
        async with self.uow() as tx:
            await tx.outbox.cleanup(retention)
