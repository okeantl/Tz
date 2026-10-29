import hashlib
import json
import uuid

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.broker import PAYMENTS_NEW
from src.core.database import get_db
from src.core.exceptions import IdempotencyConflictError, NotFoundError
from src.models import OutboxEvent, Payment
from src.modules.payments.schemas import PaymentCreate, PaymentCreatedEvent


def request_hash(data: PaymentCreate) -> str:
    body = json.dumps(data.model_dump(mode="json"), sort_keys=True)
    return hashlib.sha256(body.encode()).hexdigest()


class PaymentService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def create_payment(
        self, data: PaymentCreate, idempotency_key: str
    ) -> tuple[Payment, bool]:
        body_hash = request_hash(data)

        existing = await self._get_by_idempotency_key(idempotency_key)
        if existing is not None:
            return self._check_same_request(existing, body_hash), False

        payment = Payment(
            id=uuid.uuid4(),
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            meta=data.metadata,
            idempotency_key=idempotency_key,
            request_hash=body_hash,
            webhook_url=str(data.webhook_url),
        )
        event_id = uuid.uuid4()
        event = OutboxEvent(
            id=event_id,
            aggregate_type="payment",
            aggregate_id=payment.id,
            event_type=PAYMENTS_NEW,
            payload=PaymentCreatedEvent(
                event_id=event_id, payment_id=payment.id
            ).model_dump(mode="json"),
        )
        self.db.add(payment)
        self.db.add(event)
        try:
            await self.db.commit()
        except IntegrityError:
            await self.db.rollback()
            existing = await self._get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            return self._check_same_request(existing, body_hash), False

        await self.db.refresh(payment)
        return payment, True

    async def get_payment(self, payment_id: uuid.UUID) -> Payment:
        payment = await self.db.get(Payment, payment_id)
        if payment is None:
            raise NotFoundError("Payment not found")
        return payment

    async def _get_by_idempotency_key(self, key: str) -> Payment | None:
        result = await self.db.execute(
            select(Payment).where(Payment.idempotency_key == key)
        )
        return result.scalars().first()

    def _check_same_request(self, payment: Payment, body_hash: str) -> Payment:
        if payment.request_hash != body_hash:
            raise IdempotencyConflictError()
        return payment


def get_payment_service(db: AsyncSession = Depends(get_db)) -> PaymentService:
    return PaymentService(db)
