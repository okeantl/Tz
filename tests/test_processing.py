import pytest

from src.models import Payment, PaymentStatus
from src.modules.payments.service import PaymentService
from src.worker.processing import PaymentProcessor, WebhookError
from tests.test_payment_service import make_data


class FakeGateway:
    def __init__(self, status: PaymentStatus) -> None:
        self.status = status
        self.calls = 0

    async def charge(self, payment):
        self.calls += 1
        return self.status


class FakeWebhook:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list[dict] = []

    async def send(self, url, payload, webhook_id):
        if self.fail:
            raise WebhookError("500")
        self.sent.append(payload)


async def create_payment(session_factory) -> Payment:
    async with session_factory() as session:
        payment, _ = await PaymentService(session).create_payment(make_data(), "key-1")
    return payment


async def get_payment(session_factory, payment_id) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_process_payment(session_factory, status):
    payment = await create_payment(session_factory)
    gateway, webhook = FakeGateway(status), FakeWebhook()

    await PaymentProcessor(session_factory, gateway, webhook).process(payment.id)

    saved = await get_payment(session_factory, payment.id)
    assert saved.status is status
    assert saved.processed_at is not None
    assert saved.webhook_sent_at is not None
    assert webhook.sent[0]["status"] == status.value
    assert webhook.sent[0]["event"] == f"payment.{status.value}"


async def test_repeated_message_is_skipped(session_factory):
    payment = await create_payment(session_factory)
    gateway, webhook = FakeGateway(PaymentStatus.SUCCEEDED), FakeWebhook()
    processor = PaymentProcessor(session_factory, gateway, webhook)

    await processor.process(payment.id)
    await processor.process(payment.id)

    assert gateway.calls == 1
    assert len(webhook.sent) == 1


async def test_webhook_retry_does_not_charge_again(session_factory):
    payment = await create_payment(session_factory)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)

    with pytest.raises(WebhookError):
        await PaymentProcessor(session_factory, gateway, FakeWebhook(fail=True)).process(
            payment.id
        )
    saved = await get_payment(session_factory, payment.id)
    assert saved.status is PaymentStatus.SUCCEEDED
    assert saved.webhook_sent_at is None

    webhook = FakeWebhook()
    await PaymentProcessor(session_factory, gateway, webhook).process(payment.id)

    assert gateway.calls == 1
    assert len(webhook.sent) == 1
