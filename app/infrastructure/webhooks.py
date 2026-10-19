import asyncio
import hashlib
import hmac
import ipaddress
import json
import logging
import time

import httpx

from app.domain.payment import Payment
from app.services.errors import WebhookError, WebhookRejectedError

logger = logging.getLogger(__name__)

IpAddress = ipaddress.IPv4Address | ipaddress.IPv6Address


def may_be_public(host: str) -> bool:
    host = host.rstrip(".").lower()
    if host == "localhost" or host.endswith(".localhost"):
        return False
    try:
        address = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return True
    return address.is_global


class HttpNotifier:
    def __init__(
        self,
        client: httpx.AsyncClient,
        timeout: float,
        secret: bytes,
        allowed_hosts: frozenset[str] = frozenset(),
    ) -> None:
        self.client = client
        self.timeout = timeout
        self.secret = secret
        self.allowed_hosts = allowed_hosts

    async def send(self, payment: Payment) -> None:
        body = {
            "payment_id": str(payment.id),
            "status": payment.status,
            "failure_reason": payment.failure_reason,
            "amount": str(payment.amount),
            "currency": payment.currency,
            "metadata": payment.metadata,
            "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
        }
        content = json.dumps(body).encode()
        timestamp = str(int(time.time()))
        signed = timestamp.encode() + b"." + content
        signature = hmac.new(self.secret, signed, hashlib.sha256).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Id": str(payment.id),
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": f"sha256={signature}",
        }
        try:
            async with asyncio.timeout(self.timeout):
                request = self.client.build_request(
                    "POST", payment.webhook_url, content=content, headers=headers
                )
                host = request.url.host
                if host not in self.allowed_hosts and not may_be_public(host):
                    logger.warning("Webhook host %s points to a private address", host)
                    raise WebhookRejectedError("address not allowed")
                response = await self.client.send(request, stream=True)
                await response.aclose()
        except (httpx.InvalidURL, UnicodeError) as exc:
            raise WebhookRejectedError("invalid url") from exc
        except (httpx.TimeoutException, TimeoutError) as exc:
            raise WebhookError("timeout") from exc
        except (httpx.HTTPError, OSError) as exc:
            logger.warning("Webhook for payment %s failed: %r", payment.id, exc)
            raise WebhookError("connection failed") from exc
        if response.is_success:
            return
        error = f"HTTP {response.status_code}"
        if response.status_code == 410:
            raise WebhookRejectedError(error)
        raise WebhookError(error)
