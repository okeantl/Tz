import asyncio
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from src.core.exceptions import IdempotencyConflictError, NotFoundError
from src.models import Currency, OutboxEvent, Payment, PaymentStatus
from src.modules.payments.schemas import PaymentCreate
from src.modules.payments.service import PaymentService


def make_data(**kwargs) -> PaymentCreate:
    data = {
        "amount": "100.5",
        "currency": "RUB",
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "webhook_url": "http://example.com/hook",
    }
    data.update(kwargs)
    return PaymentCreate(**data)


async def count(session, model) -> int:
    result = await session.execute(select(func.count()).select_from(model))
    return result.scalar_one()


async def test_create_payment_writes_outbox_event(db_session):
    payment, created = await PaymentService(db_session).create_payment(
        make_data(), "key-1"
    )

    assert created is True
    assert payment.status is PaymentStatus.PENDING
    assert payment.amount == Decimal("100.50")
    assert payment.currency is Currency.RUB

    result = await db_session.execute(select(OutboxEvent))
    event = result.scalars().one()
    assert event.event_type == "payments.new"
    assert event.payload["payment_id"] == str(payment.id)
    assert event.published_at is None


async def test_same_key_returns_same_payment(db_session):
    service = PaymentService(db_session)
    first, _ = await service.create_payment(make_data(), "key-1")
    second, created = await service.create_payment(make_data(amount="100.50"), "key-1")

    assert created is False
    assert second.id == first.id
    assert await count(db_session, Payment) == 1
    assert await count(db_session, OutboxEvent) == 1


async def test_same_key_with_other_body_is_conflict(db_session):
    service = PaymentService(db_session)
    await service.create_payment(make_data(), "key-1")

    with pytest.raises(IdempotencyConflictError):
        await service.create_payment(make_data(amount="999.00"), "key-1")


async def test_concurrent_requests_with_same_key(session_factory):
    async def create():
        async with session_factory() as session:
            return await PaymentService(session).create_payment(make_data(), "key-1")

    results = await asyncio.gather(*(create() for _ in range(10)))

    assert len({payment.id for payment, _ in results}) == 1
    assert sum(created for _, created in results) == 1
    async with session_factory() as session:
        assert await count(session, Payment) == 1
        assert await count(session, OutboxEvent) == 1


async def test_get_payment(db_session):
    service = PaymentService(db_session)
    payment, _ = await service.create_payment(make_data(), "key-1")
    assert (await service.get_payment(payment.id)).id == payment.id

    with pytest.raises(NotFoundError):
        await service.get_payment(uuid.uuid4())
