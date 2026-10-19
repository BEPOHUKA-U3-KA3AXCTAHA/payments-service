import uuid
from datetime import UTC, datetime
from decimal import Decimal

from app.domain.payment import Currency, Payment, PaymentStatus


def payment(
    webhook_url: str = "https://hook.test/ok", status: PaymentStatus = PaymentStatus.SUCCEEDED
) -> Payment:
    now = datetime.now(UTC)
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="Test",
        metadata={},
        status=status,
        failure_reason=None,
        idempotency_key="key",
        request_hash="hash",
        webhook_url=webhook_url,
        created_at=now,
        processed_at=now,
        notified_at=None,
        last_error=None,
        attempts=1,
        next_attempt_at=now,
        gave_up_at=None,
    )
