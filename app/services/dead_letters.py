from uuid import UUID

from app.domain.payment import PaymentCreated
from app.services.interfaces import UnitOfWork


async def revive(uow: UnitOfWork, payment_ids: list[UUID] | None) -> list[UUID]:
    """Возвращает отказавшие платежи в работу, None значит все."""
    async with uow() as tx:
        revived = await tx.payments.revive(payment_ids)
        for payment_id in revived:
            await tx.outbox.add(PaymentCreated(payment_id))
    return revived
