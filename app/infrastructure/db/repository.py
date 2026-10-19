import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import delete, extract, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.payment import Event, NewPayment, Payment, PaymentStatus
from app.infrastructure.db.engine import Sessions
from app.infrastructure.db.models import OutboxEvent, PaymentRow
from app.services.interfaces import ChargeResult, OutboxMessage, Transaction, UnitOfWork

STUCK_BATCH = 100


def to_domain(row: PaymentRow) -> Payment:
    return Payment(
        id=row.id,
        amount=row.amount,
        currency=row.currency,
        description=row.description,
        metadata=row.meta,
        status=row.status,
        failure_reason=row.failure_reason,
        idempotency_key=row.idempotency_key,
        request_hash=row.request_hash,
        webhook_url=row.webhook_url,
        created_at=row.created_at,
        processed_at=row.processed_at,
        notified_at=row.notified_at,
        last_error=row.last_error,
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        gave_up_at=row.gave_up_at,
    )


def seconds(value: float) -> timedelta:
    return timedelta(seconds=value)


def plain(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


unfinished = (PaymentRow.notified_at.is_(None), PaymentRow.gave_up_at.is_(None))
due = PaymentRow.next_attempt_at <= func.now()
event_pending = (
    select(OutboxEvent.id)
    .where(OutboxEvent.published_at.is_(None), OutboxEvent.payment_id == PaymentRow.id)
    .exists()
)


class SqlPayments:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, idempotency_key: str, new: NewPayment) -> Payment | None:
        row = await self.session.scalar(
            insert(PaymentRow)
            .values(
                {
                    PaymentRow.idempotency_key: idempotency_key,
                    PaymentRow.request_hash: new.fingerprint(),
                    PaymentRow.amount: new.amount,
                    PaymentRow.currency: new.currency,
                    PaymentRow.description: new.description,
                    PaymentRow.meta: new.metadata,
                    PaymentRow.webhook_url: new.webhook_url,
                }
            )
            .on_conflict_do_nothing(index_elements=[PaymentRow.idempotency_key])
            .returning(PaymentRow)
        )
        return None if row is None else to_domain(row)

    async def get_by_key(self, idempotency_key: str) -> Payment:
        query = select(PaymentRow).where(PaymentRow.idempotency_key == idempotency_key)
        return to_domain((await self.session.scalars(query)).one())

    async def get(self, payment_id: UUID) -> Payment | None:
        row = await self.session.get(PaymentRow, payment_id)
        return None if row is None else to_domain(row)

    async def revive(self, payment_ids: list[UUID] | None) -> list[UUID]:
        query = update(PaymentRow).where(PaymentRow.gave_up_at.is_not(None), due, ~event_pending)
        if payment_ids is not None:
            query = query.where(PaymentRow.id.in_(payment_ids))
        ids = await self.session.scalars(
            query.values(gave_up_at=None, attempts=0, next_attempt_at=func.now()).returning(
                PaymentRow.id
            )
        )
        return list(ids)

    async def gave_up_at(self, payment_ids: list[UUID]) -> dict[UUID, datetime | None]:
        rows = await self.session.execute(
            select(PaymentRow.id, PaymentRow.gave_up_at).where(PaymentRow.id.in_(payment_ids))
        )
        return dict(rows.tuples().all())

    async def count_waiting(self) -> int:
        count = await self.session.scalar(
            select(func.count()).select_from(PaymentRow).where(*unfinished, due, ~event_pending)
        )
        return count or 0

    async def take_stuck(self, stuck_after: float, limit: int) -> list[UUID]:
        stuck = (
            select(PaymentRow.id)
            .where(
                *unfinished,
                PaymentRow.next_attempt_at < func.now() - seconds(stuck_after),
                ~event_pending,
            )
            .order_by(PaymentRow.next_attempt_at)
            .limit(min(limit, STUCK_BATCH))
            .with_for_update(skip_locked=True)
        )
        ids = await self.session.scalars(
            update(PaymentRow)
            .where(PaymentRow.id.in_(stuck))
            .values(next_attempt_at=func.now())
            .returning(PaymentRow.id)
        )
        return list(ids)


class SqlAttempts:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def claim(self, payment_id: UUID, token: UUID, lease: float) -> Payment | None:
        row = await self.session.scalar(
            update(PaymentRow)
            .where(PaymentRow.id == payment_id, *unfinished, due)
            .values(
                attempts=PaymentRow.attempts + 1,
                lock_token=token,
                next_attempt_at=func.now() + seconds(lease),
            )
            .returning(PaymentRow)
        )
        return None if row is None else to_domain(row)

    async def wait_time(self, payment_id: UUID) -> float:
        wait = await self.session.scalar(
            select(extract("epoch", PaymentRow.next_attempt_at - func.now())).where(
                PaymentRow.id == payment_id
            )
        )
        return max(float(wait or 0), 0.0)

    async def get_locked(self, payment_id: UUID, token: UUID) -> Payment | None:
        row = await self.session.scalar(
            select(PaymentRow)
            .where(PaymentRow.id == payment_id, PaymentRow.lock_token == token)
            .with_for_update()
        )
        return None if row is None else to_domain(row)

    async def release(self, payment_id: UUID, token: UUID, error: str, delay: float) -> None:
        await self.locked_update(
            payment_id, token, last_error=error, next_attempt_at=func.now() + seconds(delay)
        )

    async def set_result(
        self, payment_id: UUID, token: UUID, result: ChargeResult, lease: float
    ) -> Payment | None:
        row = await self.locked_update(
            payment_id,
            token,
            status=result.status,
            failure_reason=result.reason,
            processed_at=func.now(),
            attempts=1,
            lock_token=token,
            next_attempt_at=func.now() + seconds(lease),
        )
        return None if row is None else to_domain(row)

    async def set_notified(self, payment_id: UUID, token: UUID) -> None:
        await self.locked_update(payment_id, token, notified_at=func.now(), last_error=None)

    async def give_up(self, payment_id: UUID, token: UUID, error: str, attempts: int) -> datetime:
        row = await self.locked_update(
            payment_id, token, gave_up_at=func.now(), last_error=error, attempts=attempts
        )
        if row is None or row.gave_up_at is None:
            raise LookupError(f"payment {payment_id} is not locked by {token}")
        return row.gave_up_at

    async def claim_abandoned(
        self, token: UUID, lease: float, older_than: float, limit: int
    ) -> list[Payment]:
        abandoned = (
            select(PaymentRow.id)
            .where(
                PaymentRow.status == PaymentStatus.PENDING,
                PaymentRow.gave_up_at.is_not(None),
                PaymentRow.next_attempt_at <= func.now() - seconds(older_than),
                ~event_pending,
            )
            .order_by(PaymentRow.next_attempt_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = await self.session.scalars(
            update(PaymentRow)
            .where(PaymentRow.id.in_(abandoned))
            .values(lock_token=token, next_attempt_at=func.now() + seconds(lease))
            .returning(PaymentRow)
        )
        return [to_domain(row) for row in rows]

    async def resume(self, payment_id: UUID, token: UUID, result: ChargeResult) -> bool:
        row = await self.locked_update(
            payment_id,
            token,
            status=result.status,
            failure_reason=result.reason,
            processed_at=func.now(),
            gave_up_at=None,
            last_error=None,
            attempts=0,
        )
        return row is not None

    async def locked_update(
        self, payment_id: UUID, token: UUID, **values: Any
    ) -> PaymentRow | None:
        return await self.session.scalar(
            update(PaymentRow)
            .where(PaymentRow.id == payment_id, PaymentRow.lock_token == token)
            .values({"lock_token": None, "next_attempt_at": func.now(), **values})
            .returning(PaymentRow)
        )


class SqlOutbox:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, event: Event) -> None:
        payload = {key: plain(value) for key, value in asdict(event).items()}
        self.session.add(
            OutboxEvent(event_type=event.event_type, payment_id=event.payment_id, payload=payload)
        )

    async def take(self, limit: int, lease: float) -> list[OutboxMessage]:
        free = or_(OutboxEvent.locked_until.is_(None), OutboxEvent.locked_until < func.now())
        batch = (
            select(OutboxEvent.id)
            .where(OutboxEvent.published_at.is_(None), free)
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = await self.session.scalars(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(batch))
            .values(locked_until=func.now() + seconds(lease))
            .returning(OutboxEvent)
        )
        messages = [OutboxMessage(row.id, row.event_type, row.payload) for row in rows]
        return sorted(messages, key=lambda m: m.id)

    async def mark_published(self, ids: list[int]) -> None:
        if ids:
            await self.session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id.in_(ids))
                .values(published_at=func.now(), locked_until=None)
            )

    async def release(self, ids: list[int]) -> None:
        if ids:
            await self.session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id.in_(ids), OutboxEvent.published_at.is_(None))
                .values(locked_until=None)
            )

    async def cleanup(self, retention: float) -> None:
        await self.session.execute(
            delete(OutboxEvent).where(OutboxEvent.published_at < func.now() - seconds(retention))
        )


@dataclass(frozen=True)
class SqlTransaction:
    payments: SqlPayments
    attempts: SqlAttempts
    outbox: SqlOutbox


def unit_of_work(sessions: Sessions, timeout: float | None = None) -> UnitOfWork:
    @asynccontextmanager
    async def transaction() -> AsyncIterator[Transaction]:
        async with asyncio.timeout(timeout), sessions.begin() as session:
            yield SqlTransaction(SqlPayments(session), SqlAttempts(session), SqlOutbox(session))

    return transaction
