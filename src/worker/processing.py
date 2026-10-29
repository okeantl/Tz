import asyncio
import random
import uuid
from typing import Any

import httpx
import structlog
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.models import Payment, PaymentStatus
from src.modules.payments.schemas import PaymentRead

logger = structlog.get_logger(__name__)


class PaymentNotFoundError(Exception):
    pass


class WebhookError(Exception):
    pass


class PaymentGateway:
    def __init__(self, min_delay: float, max_delay: float, success_rate: float) -> None:
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.success_rate = success_rate

    async def charge(self, payment: Payment) -> PaymentStatus:
        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
        if random.random() < self.success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def send(self, url: str, payload: dict[str, Any], webhook_id: str) -> None:
        try:
            response = await self.client.post(
                url, json=payload, headers={"X-Webhook-Id": webhook_id}
            )
        except httpx.HTTPError as e:
            raise WebhookError(f"request to {url} failed: {e!r}") from e
        if not response.is_success:
            raise WebhookError(f"{url} responded with {response.status_code}")


def webhook_payload(payment: Payment) -> dict[str, Any]:
    data = PaymentRead.model_validate(payment).model_dump(mode="json")
    return {"event": f"payment.{payment.status.value}", **data}


class PaymentProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook: WebhookSender,
    ) -> None:
        self.session_factory = session_factory
        self.gateway = gateway
        self.webhook = webhook

    async def process(self, payment_id: uuid.UUID) -> None:
        async with self.session_factory() as db:
            payment = await db.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFoundError(f"payment {payment_id} not found")

        if payment.webhook_sent_at is not None:
            logger.info("payment_already_processed", payment_id=str(payment_id))
            return

        if payment.status is PaymentStatus.PENDING:
            status = await self.gateway.charge(payment)
            payment = await self._set_status(payment.id, status)
            logger.info(
                "payment_processed",
                payment_id=str(payment.id),
                status=payment.status.value,
            )

        await self.webhook.send(
            payment.webhook_url, webhook_payload(payment), webhook_id=str(payment.id)
        )
        async with self.session_factory() as db:
            await db.execute(
                update(Payment)
                .where(Payment.id == payment.id)
                .values(webhook_sent_at=func.now())
            )
            await db.commit()
        logger.info("webhook_sent", payment_id=str(payment.id), url=payment.webhook_url)

    async def _set_status(self, payment_id: uuid.UUID, status: PaymentStatus) -> Payment:
        async with self.session_factory() as db:
            await db.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                .values(status=status, processed_at=func.now())
            )
            await db.commit()
            payment = await db.get(Payment, payment_id, populate_existing=True)
        if payment is None:
            raise PaymentNotFoundError(f"payment {payment_id} not found")
        return payment
