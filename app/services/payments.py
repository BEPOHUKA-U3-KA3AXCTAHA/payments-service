import logging
from uuid import UUID

from app.domain.payment import NewPayment, Payment, PaymentCreated
from app.services.errors import IdempotencyConflictError
from app.services.interfaces import PaymentQueue, UnitOfWork

logger = logging.getLogger(__name__)


async def create_payment(uow: UnitOfWork, idempotency_key: str, new: NewPayment) -> Payment:
    async with uow() as tx:
        payment = await tx.payments.add(idempotency_key, new)
        if payment is not None:
            await tx.outbox.add(PaymentCreated(payment.id))
            return payment
        payment = await tx.payments.get_by_key(idempotency_key)
        if not payment.same_request(new):
            raise IdempotencyConflictError
    return payment


async def get_payment(uow: UnitOfWork, payment_id: UUID) -> Payment | None:
    async with uow() as tx:
        return await tx.payments.get(payment_id)


async def requeue_stuck(uow: UnitOfWork, queue: PaymentQueue, stuck_after: float) -> int:
    ready = await queue.ready_count()
    async with uow() as tx:
        missing = await tx.payments.count_waiting() - ready
        if missing <= 0:
            return 0
        stuck = await tx.payments.take_stuck(stuck_after, missing)
        for payment_id in stuck:
            await tx.outbox.add(PaymentCreated(payment_id))
    if stuck:
        logger.warning("Requeued %s stuck payments", len(stuck))
    return len(stuck)
