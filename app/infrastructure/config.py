import logging
from typing import Annotated

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from app.services.retry import RetryPolicy


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    db_timeout: float = Field(default=5.0, gt=0)
    prefetch: int = Field(default=25, ge=1)

    max_attempts: int = Field(default=3, ge=1)
    retry_delay: float = Field(default=5.0, gt=0)
    gateway_timeout: float = Field(default=10.0, gt=0)
    webhook_timeout: float = Field(default=5.0, gt=0)
    outbox_poll_interval: float = Field(default=1.0, gt=0)
    outbox_retention: float = Field(default=24 * 3600, gt=0)
    stuck_after: float = Field(default=300.0, gt=0)
    sweep_interval: float = Field(default=60.0, gt=0)
    webhook_allowed_hosts: Annotated[frozenset[str], NoDecode] = frozenset()

    @field_validator("webhook_allowed_hosts", mode="before")
    @classmethod
    def split_hosts(cls, value: object) -> object:
        if isinstance(value, str):
            return {host.strip().lower() for host in value.split(",") if host.strip()}
        return value

    def retry_policy(self) -> RetryPolicy:
        return RetryPolicy(self.max_attempts, self.retry_delay)

    @property
    def lease_margin(self) -> float:
        return 2 * self.db_timeout + 5


class ApiSettings(Settings):
    api_key: SecretStr = Field(min_length=1)


class ConsumerSettings(Settings):
    webhook_secret: SecretStr = Field(min_length=1)
    gateway_url: str = "http://localhost:8001"
    reconcile_interval: float = Field(default=60.0, gt=0)


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
