import json
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.domain.payment import Currency, NewPayment, PaymentStatus, Stage
from app.infrastructure.webhooks import may_be_public

METADATA_MAX_BYTES = 16 * 1024


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str = Field(max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("metadata")
    @classmethod
    def check_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value, ensure_ascii=False).encode()) > METADATA_MAX_BYTES:
            raise ValueError(f"must be at most {METADATA_MAX_BYTES} bytes as JSON")
        return value

    def private_webhook(self, allowed_hosts: frozenset[str]) -> bool:
        host = (self.webhook_url.host or "").strip("[]")
        return host not in allowed_hosts and not may_be_public(host)

    def to_domain(self) -> NewPayment:
        return NewPayment(
            self.amount, self.currency, self.description, self.metadata, str(self.webhook_url)
        )


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentRead(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    failure_reason: str | None
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None
    notified_at: datetime | None
    stage: Stage
    attempts: int
    gave_up_at: datetime | None
    last_error: str | None
