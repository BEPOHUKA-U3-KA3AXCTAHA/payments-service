from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from app.domain.payment import Event, NewPayment, Payment, PaymentStatus


@dataclass(frozen=True)
class ChargeResult:
    status: PaymentStatus
    reason: str | None = None


class Payments(Protocol):
    async def add(self, idempotency_key: str, new: NewPayment) -> Payment | None: ...

    async def get(self, payment_id: UUID) -> Payment | None: ...

    async def get_by_key(self, idempotency_key: str) -> Payment: ...

    async def revive(self, payment_ids: list[UUID] | None) -> list[UUID]: ...

    async def gave_up_at(self, payment_ids: list[UUID]) -> dict[UUID, datetime | None]:
        """Время отказа каждого из найденных платежей, None у тех, что не в отказе."""
        ...

    async def count_waiting(self) -> int:
        """Сколько платежей ждут обработки, и сообщение о них уже должно быть в очереди."""
        ...

    async def take_stuck(self, stuck_after: float, limit: int) -> list[UUID]: ...


class Attempts(Protocol):
    """Захват платежа на попытку и все, что пишет владелец лока."""

    async def claim(self, payment_id: UUID, token: UUID, lease: float) -> Payment | None: ...

    async def wait_time(self, payment_id: UUID) -> float: ...

    async def get_locked(self, payment_id: UUID, token: UUID) -> Payment | None:
        """Свежий платеж под блокировкой строки до конца транзакции. None, если лок потерян."""
        ...

    async def release(self, payment_id: UUID, token: UUID, error: str, delay: float) -> None: ...

    async def set_result(
        self, payment_id: UUID, token: UUID, result: ChargeResult, lease: float
    ) -> Payment | None: ...

    async def set_notified(self, payment_id: UUID, token: UUID) -> None: ...

    async def give_up(self, payment_id: UUID, token: UUID, error: str, attempts: int) -> datetime:
        """Вызывать после get_locked в той же транзакции. Возвращает время отказа."""
        ...

    async def claim_abandoned(
        self, token: UUID, lease: float, older_than: float, limit: int
    ) -> list[Payment]: ...

    async def resume(self, payment_id: UUID, token: UUID, result: ChargeResult) -> bool: ...


@dataclass(frozen=True)
class OutboxMessage:
    id: int
    event_type: str
    payload: dict[str, Any]


class Outbox(Protocol):
    async def add(self, event: Event) -> None: ...

    async def take(self, limit: int, lease: float) -> list[OutboxMessage]: ...

    async def mark_published(self, ids: list[int]) -> None: ...

    async def release(self, ids: list[int]) -> None: ...

    async def cleanup(self, retention: float) -> None: ...


class Transaction(Protocol):
    @property
    def payments(self) -> Payments: ...

    @property
    def attempts(self) -> Attempts: ...

    @property
    def outbox(self) -> Outbox: ...


class UnitOfWork(Protocol):
    def __call__(self) -> AbstractAsyncContextManager[Transaction]: ...


class Gateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus: ...


class Notifier(Protocol):
    async def send(self, payment: Payment) -> None: ...


class Publisher(Protocol):
    async def publish(self, message: OutboxMessage) -> None: ...


class PaymentQueue(Protocol):
    async def ready_count(self) -> int:
        """Сообщения, которые лежат в очереди и еще не выданы consumer'у."""
        ...
