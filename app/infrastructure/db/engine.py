import sqlalchemy.exc
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.infrastructure.config import Settings

Sessions = async_sessionmaker[AsyncSession]

# так выглядит недоступная база (ошибки подключения asyncpg алхимия не оборачивает)
DB_UNAVAILABLE: tuple[type[Exception], ...] = (
    sqlalchemy.exc.OperationalError,
    sqlalchemy.exc.InterfaceError,
    sqlalchemy.exc.TimeoutError,
    OSError,
    TimeoutError,
)


def create_db_engine(settings: Settings, pool_size: int = 5) -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=pool_size,
        pool_pre_ping=True,
        pool_timeout=settings.db_timeout,
        connect_args={"timeout": settings.db_timeout, "command_timeout": settings.db_timeout},
    )


def session_factory(engine: AsyncEngine) -> Sessions:
    return async_sessionmaker(engine, expire_on_commit=False)


async def ping(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
