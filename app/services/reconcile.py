import asyncio
import logging
from dataclasses import dataclass
from uuid import UUID, uuid4

from app.domain.payment import Payment, PaymentCreated
from app.services.errors import ChargePendingError
from app.services.interfaces import Gateway, UnitOfWork
from app.services.processing import charge

logger = logging.getLogger(__name__)


@dataclass
class Reconciler:
    uow: UnitOfWork
    gateway: Gateway
    gateway_timeout: float
    lease_margin: float
    older_than: float
    batch_size: int

    async def run(self) -> int:
        resolved = 0
        while payments := await self.claim():
            results = await asyncio.gather(
                *(self.resolve(payment, token) for payment, token in payments),
                return_exceptions=True,
            )
            for (payment, _), result in zip(payments, results, strict=True):
                if isinstance(result, BaseException):
                    logger.warning("Reconcile of %s failed: %r", payment.id, result)
                elif result:
                    resolved += 1
        return resolved

    async def claim(self) -> list[tuple[Payment, UUID]]:
        token = uuid4()
        lease = self.gateway_timeout + self.lease_margin
        async with self.uow() as tx:
            payments = await tx.attempts.claim_abandoned(
                token, lease, self.older_than, self.batch_size
            )
        return [(payment, token) for payment in payments]

    async def resolve(self, payment: Payment, token: UUID) -> bool:
        try:
            result = await charge(self.gateway, payment, self.gateway_timeout)
        except ChargePendingError:
            await self.leave(payment, token, "charge pending")
            return False
        except Exception as exc:
            logger.warning("Payment %s still unresolved: %r", payment.id, exc)
            await self.leave(payment, token, "gateway unavailable")
            return False

        async with self.uow() as tx:
            resumed = await tx.attempts.resume(payment.id, token, result)
            if resumed:
                await tx.outbox.add(PaymentCreated(payment.id))
        logger.info("Payment %s resolved by gateway as %s", payment.id, result.status)
        return resumed

    async def leave(self, payment: Payment, token: UUID, error: str) -> None:
        async with self.uow() as tx:
            await tx.attempts.release(payment.id, token, error, 0)
