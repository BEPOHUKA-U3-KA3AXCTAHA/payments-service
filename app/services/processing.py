import asyncio
import logging
from dataclasses import dataclass
from uuid import UUID, uuid4

from app.domain.payment import Payment, PaymentDeadLettered, PaymentStatus
from app.services.errors import (
    ChargePendingError,
    GatewayRejectedError,
    PaymentNotFoundError,
    WebhookError,
    WebhookRejectedError,
)
from app.services.interfaces import ChargeResult, Gateway, Notifier, UnitOfWork
from app.services.retry import RetryPolicy

logger = logging.getLogger(__name__)


def outcome(status: PaymentStatus) -> ChargeResult:
    if status == PaymentStatus.PENDING:
        raise ChargePendingError
    return ChargeResult(status, "declined" if status == PaymentStatus.FAILED else None)


async def charge(gateway: Gateway, payment: Payment, timeout: float) -> ChargeResult:
    try:
        async with asyncio.timeout(timeout):
            status = await gateway.charge(payment)
    except GatewayRejectedError as exc:
        return ChargeResult(PaymentStatus.FAILED, f"gateway rejected: {exc}")
    return outcome(status)


@dataclass(frozen=True)
class Retry:
    delay: float
    error: str


@dataclass
class PaymentProcessor:
    uow: UnitOfWork
    gateway: Gateway
    notifier: Notifier
    policy: RetryPolicy
    gateway_timeout: float
    webhook_timeout: float
    lease_margin: float

    async def process(self, payment_id: UUID) -> Retry | None:
        token = uuid4()
        lease = max(self.gateway_timeout, self.webhook_timeout) + self.lease_margin
        async with self.uow() as tx:
            payment = await tx.attempts.claim(payment_id, token, lease)
        if payment is None:
            return await self.skip(payment_id)

        if self.policy.interrupted(payment.attempts):
            return await self.fail(payment.id, token, "processing interrupted", retry=False)

        try:
            return await self.attempt(payment, token)
        except Exception:
            logger.exception("Payment %s failed", payment.id)
            return await self.fail(payment.id, token, "internal error")

    async def skip(self, payment_id: UUID) -> Retry | None:
        async with self.uow() as tx:
            payment = await tx.payments.get(payment_id)
            wait = await tx.attempts.wait_time(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        if payment.finished:
            return None
        return Retry(self.policy.round_up(wait), "not due yet")

    async def attempt(self, payment: Payment, token: UUID) -> Retry | None:
        if payment.status == PaymentStatus.PENDING:
            try:
                result = await charge(self.gateway, payment, self.gateway_timeout)
            except ChargePendingError:
                return await self.fail(payment.id, token, "charge pending")
            except Exception as exc:
                logger.warning("Gateway failed on %s: %r", payment.id, exc)
                return await self.fail(payment.id, token, "gateway unavailable")
            async with self.uow() as tx:
                lease = self.webhook_timeout + self.lease_margin
                charged = await tx.attempts.set_result(payment.id, token, result, lease)
            if charged is None:
                logger.warning("Lost lock on %s, another consumer took it", payment.id)
                return None
            payment = charged

        try:
            await self.notifier.send(payment)
        except WebhookRejectedError as exc:
            return await self.fail(payment.id, token, str(exc), retry=False)
        except WebhookError as exc:
            return await self.fail(payment.id, token, str(exc))
        async with self.uow() as tx:
            await tx.attempts.set_notified(payment.id, token)
        return None

    async def fail(
        self, payment_id: UUID, token: UUID, error: str, *, retry: bool = True
    ) -> Retry | None:
        async with self.uow() as tx:
            payment = await tx.attempts.get_locked(payment_id, token)
            if payment is None:
                logger.warning("Payment %s is no longer held by this attempt", payment_id)
                return None
            if retry and not self.policy.exhausted(payment.attempts):
                delay = self.policy.delay(payment.attempts)
                await tx.attempts.release(payment_id, token, error, delay)
                return Retry(delay, error)
            attempts = min(payment.attempts, self.policy.max_attempts)
            gave_up_at = await tx.attempts.give_up(payment_id, token, error, attempts)
            letter = PaymentDeadLettered(payment_id, payment.stage, attempts, error, gave_up_at)
            await tx.outbox.add(letter)
        logger.error(
            "Payment %s -> DLQ after %d %s attempts: %s", payment_id, attempts, payment.stage, error
        )
        return None
