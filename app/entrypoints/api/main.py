import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.entrypoints.api.auth import ApiKeyMiddleware
from app.entrypoints.api.routes import router
from app.infrastructure.config import ApiSettings
from app.infrastructure.db.engine import DB_UNAVAILABLE, create_db_engine, session_factory
from app.infrastructure.db.repository import unit_of_work

logger = logging.getLogger(__name__)


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    settings = settings or ApiSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_db_engine(settings)
        app.state.engine = engine
        app.state.uow = unit_of_work(session_factory(engine), settings.db_timeout)
        yield
        await engine.dispose()

    app = FastAPI(title="Payments", lifespan=lifespan)
    app.state.settings = settings
    app.add_middleware(ApiKeyMiddleware, api_key=settings.api_key.get_secret_value())
    app.include_router(router)

    async def database_unavailable(request: Request, exc: Exception) -> JSONResponse:
        logger.warning("Database unavailable on %s %s: %r", request.method, request.url.path, exc)
        return JSONResponse(status_code=503, content={"detail": "Database unavailable"})

    for error in DB_UNAVAILABLE:
        app.add_exception_handler(error, database_unavailable)

    return app
