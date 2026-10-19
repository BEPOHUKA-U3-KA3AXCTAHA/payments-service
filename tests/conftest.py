from collections.abc import AsyncIterator, Iterator

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer
from testcontainers.core.config import testcontainers_config

from app.entrypoints.api.main import create_app
from app.infrastructure.config import ApiSettings, ConsumerSettings
from app.infrastructure.db.engine import Sessions, create_db_engine, session_factory


class AllSettings(ApiSettings, ConsumerSettings):
    pass


@pytest.fixture(scope="session")
def settings() -> Iterator[AllSettings]:
    # хост, который угадывает testcontainers (шлюз docker-сети), бывает недоступен
    testcontainers_config.tc_host_override = testcontainers_config.tc_host_override or "127.0.0.1"
    with (
        PostgresContainer("postgres:17-alpine", driver="asyncpg") as postgres,
        RabbitMqContainer("rabbitmq:4.1-alpine") as rabbitmq,
    ):
        host, port = rabbitmq.get_container_host_ip(), rabbitmq.get_exposed_port(5672)
        settings = AllSettings(
            api_key="test-key",
            webhook_secret="test-secret",
            webhook_allowed_hosts={"hook.test"},
            database_url=postgres.get_connection_url(),
            rabbitmq_url=f"amqp://guest:guest@{host}:{port}/",
            outbox_poll_interval=0.05,
            retry_delay=0.1,
            gateway_timeout=0.5,
            webhook_timeout=0.5,
            sweep_interval=3600,
            reconcile_interval=3600,
        )
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", settings.database_url)
        command.upgrade(config, "head")
        yield settings


@pytest.fixture(scope="session")
async def sessions(settings: ApiSettings) -> AsyncIterator[Sessions]:
    engine = create_db_engine(settings)
    yield session_factory(engine)
    await engine.dispose()


@pytest.fixture
async def client(settings: ApiSettings) -> AsyncIterator[AsyncClient]:
    app = create_app(settings)
    transport = ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=transport, base_url="http://test") as client,
    ):
        client.headers["X-API-Key"] = settings.api_key.get_secret_value()
        yield client
