import uuid

from fastapi import APIRouter, Depends, Header, Response, status

from src.modules.payments.dependencies import verify_api_key
from src.modules.payments.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from src.modules.payments.service import PaymentService, get_payment_service

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post(
    "", response_model=PaymentAccepted, status_code=status.HTTP_202_ACCEPTED
)
async def create_payment(
    data: PaymentCreate,
    response: Response,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=255),
    service: PaymentService = Depends(get_payment_service),
):
    payment, created = await service.create_payment(data, idempotency_key)
    if not created:
        response.headers["Idempotent-Replayed"] = "true"
    return payment


@router.get("/{payment_id}", response_model=PaymentRead)
async def get_payment(
    payment_id: uuid.UUID,
    service: PaymentService = Depends(get_payment_service),
):
    return await service.get_payment(payment_id)
