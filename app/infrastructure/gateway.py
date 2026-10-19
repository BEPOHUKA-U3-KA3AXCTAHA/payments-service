import httpx

from app.domain.payment import Payment, PaymentStatus
from app.services.errors import GatewayRejectedError

DECLINED = (402, 422)


class HttpGateway:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def charge(self, payment: Payment) -> PaymentStatus:
        response = await self.client.post(
            "/charges",
            json={"amount": str(payment.amount), "currency": payment.currency},
            headers={"Idempotency-Key": str(payment.id)},
        )
        if response.status_code in DECLINED:
            raise GatewayRejectedError(f"HTTP {response.status_code}")
        response.raise_for_status()
        return PaymentStatus(response.json()["status"])
