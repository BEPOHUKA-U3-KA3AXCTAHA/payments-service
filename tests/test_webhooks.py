import asyncio
import hashlib
import hmac
import time

import httpx
import pytest

from app.infrastructure.webhooks import HttpNotifier
from app.services.errors import WebhookError, WebhookRejectedError
from tests.factories import payment


def notifier(client: httpx.AsyncClient) -> HttpNotifier:
    return HttpNotifier(
        client, timeout=0.1, secret=b"secret", allowed_hosts=frozenset({"hook.test"})
    )


@pytest.mark.parametrize(
    ("result", "error", "kind"),
    [
        (httpx.ConnectError("refused 10.0.0.5"), "connection failed", WebhookError),
        (httpx.ReadTimeout("slow"), "timeout", WebhookError),
        (httpx.Response(500), "HTTP 500", WebhookError),
        (httpx.Response(429), "HTTP 429", WebhookError),
        (None, "timeout", WebhookError),
        (httpx.Response(410), "HTTP 410", WebhookRejectedError),
        (httpx.Response(404), "HTTP 404", WebhookError),
        (httpx.Response(401), "HTTP 401", WebhookError),
        (
            httpx.Response(301, headers={"Location": "https://hook.test/new"}),
            "HTTP 301",
            WebhookError,
        ),
    ],
)
async def test_failed_webhook(
    result: httpx.Response | Exception | None, error: str, kind: type[WebhookError]
):
    async def reply(request):
        if result is None:
            await asyncio.sleep(1)
        if isinstance(result, Exception):
            raise result
        return result

    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
        with pytest.raises(WebhookError) as exc:
            await notifier(client).send(payment())
    assert str(exc.value) == error
    assert type(exc.value) is kind


async def test_signature():
    requests: list[httpx.Request] = []

    def reply(request):
        requests.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
        await notifier(client).send(payment())
    headers = requests[0].headers
    timestamp = headers["X-Webhook-Timestamp"]
    assert abs(time.time() - int(timestamp)) < 5
    signed = timestamp.encode() + b"." + requests[0].content
    expected = hmac.new(b"secret", signed, hashlib.sha256).hexdigest()
    assert headers["X-Webhook-Signature"] == f"sha256={expected}"


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8080/hook",
        "http://[::1]/hook",
    ],
)
async def test_private_address(url: str):
    requests: list[httpx.Request] = []

    def reply(request):
        requests.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
        with pytest.raises(WebhookRejectedError):
            await notifier(client).send(payment(url))
    assert requests == []
