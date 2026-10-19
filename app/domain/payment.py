import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, ClassVar
from uuid import UUID


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Stage(StrEnum):
    CHARGE = "charge"
    WEBHOOK = "webhook"


@dataclass
class NewPayment:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str

    def fingerprint(self) -> str:
        body = {
            "amount": str(self.amount.quantize(Decimal("0.01"))),
            "currency": self.currency.value,
            "description": self.description,
            "metadata": self.metadata,
            "webhook_url": self.webhook_url,
        }
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass
class Payment:
    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    failure_reason: str | None
    idempotency_key: str
    request_hash: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    notified_at: datetime | None
    last_error: str | None
    attempts: int
    next_attempt_at: datetime
    gave_up_at: datetime | None

    @property
    def stage(self) -> Stage:
        return Stage.CHARGE if self.status == PaymentStatus.PENDING else Stage.WEBHOOK

    @property
    def finished(self) -> bool:
        return self.notified_at is not None or self.gave_up_at is not None

    def same_request(self, new: NewPayment) -> bool:
        return self.request_hash == new.fingerprint()


@dataclass(frozen=True)
class PaymentCreated:
    event_type: ClassVar[str] = "payment.created"

    payment_id: UUID


@dataclass(frozen=True)
class PaymentDeadLettered:
    event_type: ClassVar[str] = "payment.dead_lettered"

    payment_id: UUID
    stage: Stage
    attempts: int
    error: str
    gave_up_at: datetime


Event = PaymentCreated | PaymentDeadLettered
