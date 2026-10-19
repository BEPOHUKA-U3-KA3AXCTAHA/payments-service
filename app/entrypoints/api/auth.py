import secrets

from fastapi.security import APIKeyHeader
from starlette.datastructures import Headers
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class ApiKeyMiddleware:
    def __init__(self, app: ASGIApp, api_key: str) -> None:
        self.app = app
        self.expected = api_key.encode()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            key = Headers(scope=scope).get("x-api-key")
            if key is None or not secrets.compare_digest(key.encode(), self.expected):
                response = JSONResponse({"detail": "Invalid API key"}, status_code=401)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
