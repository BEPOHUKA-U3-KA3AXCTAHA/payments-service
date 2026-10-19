"""Эмулятор платежного шлюза, списания хранит в SQLite по ключу идемпотентности."""

import asyncio
import random
import sqlite3
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path as FilePath
from typing import Annotated, Any, Literal

from fastapi import FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

Status = Literal["succeeded", "failed"]
CONFLICT: dict[int | str, dict[str, Any]] = {409: {"description": "Key reused for another charge"}}
SELECT = "SELECT amount, currency, status, ready_at FROM charges WHERE idempotency_key = ?"


class EmulatorSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    db_path: FilePath = FilePath("gateway.db")
    min_delay: float = 2.0
    max_delay: float = 5.0
    success_rate: float = Field(default=0.9, ge=0, le=1)


class ChargeRequest(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: str = Field(min_length=3, max_length=3)


class ChargeResult(BaseModel):
    status: Status


@dataclass(frozen=True)
class Charge:
    amount: Decimal
    currency: str
    status: Status
    ready_at: float


class Charges:
    def __init__(self, path: FilePath) -> None:
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.lock = threading.Lock()
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS charges (idempotency_key TEXT PRIMARY KEY,"
            " amount TEXT NOT NULL, currency TEXT NOT NULL, status TEXT NOT NULL,"
            " ready_at REAL NOT NULL)"
        )

    def remember(self, key: str, new: Charge) -> Charge:
        # INSERT OR IGNORE: повтор с тем же ключом получит первое списание
        with self.lock:
            self.db.execute(
                "INSERT OR IGNORE INTO charges VALUES (?, ?, ?, ?, ?)",
                (key, str(new.amount), new.currency, new.status, new.ready_at),
            )
            row = self.db.execute(SELECT, (key,)).fetchone()
        return Charge(Decimal(row[0]), *row[1:])

    def ping(self) -> None:
        with self.lock:
            self.db.execute("SELECT 1")

    def close(self) -> None:
        with self.lock:
            self.db.close()


def create_app(settings: EmulatorSettings | None = None) -> FastAPI:
    settings = settings or EmulatorSettings()
    charges = Charges(settings.db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        charges.close()

    app = FastAPI(title="Payment gateway emulator", lifespan=lifespan)

    @app.post("/charges", responses=CONFLICT)
    async def charge(
        body: ChargeRequest,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    ) -> ChargeResult:
        outcome: Status = "succeeded" if random.random() < settings.success_rate else "failed"
        delay = random.uniform(settings.min_delay, settings.max_delay)
        new = Charge(body.amount, body.currency, outcome, time.time() + delay)
        existing = await asyncio.to_thread(charges.remember, idempotency_key, new)
        if (existing.amount, existing.currency) != (body.amount, body.currency):
            raise HTTPException(status.HTTP_409_CONFLICT, "Idempotency-Key used for another charge")
        await asyncio.sleep(max(existing.ready_at - time.time(), 0))
        return ChargeResult(status=existing.status)

    @app.get("/health")
    async def health() -> dict[str, str]:
        await asyncio.to_thread(charges.ping)
        return {"status": "ok"}

    return app
