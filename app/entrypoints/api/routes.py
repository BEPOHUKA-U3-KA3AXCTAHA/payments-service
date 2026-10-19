import asyncio
from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Security, status

from app.entrypoints.api.auth import api_key_header
from app.entrypoints.api.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from app.infrastructure.config import ApiSettings
from app.infrastructure.db.engine import ping
from app.services import payments
from app.services.errors import IdempotencyConflictError
from app.services.interfaces import UnitOfWork


def get_uow(request: Request) -> UnitOfWork:
    return cast(UnitOfWork, request.app.state.uow)


def get_settings(request: Request) -> ApiSettings:
    return cast(ApiSettings, request.app.state.settings)


router = APIRouter(
    prefix="/api/v1",
    dependencies=[Security(api_key_header)],
    responses={
        401: {"description": "Invalid API key"},
        503: {"description": "Database unavailable"},
    },
)
Uow = Annotated[UnitOfWork, Depends(get_uow)]
Config = Annotated[ApiSettings, Depends(get_settings)]


@router.post(
    "/payments",
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"description": "Idempotency-Key reused with a different body"}},
)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    uow: Uow,
    settings: Config,
) -> PaymentAccepted:
    if body.private_webhook(settings.webhook_allowed_hosts):
        raise HTTPException(422, "webhook_url points to a private address")
    try:
        payment = await payments.create_payment(uow, idempotency_key, body.to_domain())
    except IdempotencyConflictError:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Idempotency-Key reused with a different body"
        ) from None
    return PaymentAccepted.model_validate(payment)


@router.get("/payments/{payment_id}", responses={404: {"description": "Payment not found"}})
async def get_payment(payment_id: UUID, uow: Uow) -> PaymentRead:
    payment = await payments.get_payment(uow, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return PaymentRead.model_validate(payment)


@router.get("/health", responses={503: {"description": "Database unavailable"}})
async def health(request: Request) -> dict[str, str]:
    try:
        async with asyncio.timeout(2):
            await ping(request.app.state.engine)
    except Exception:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Database unavailable") from None
    return {"status": "ok"}
