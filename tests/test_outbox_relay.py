from sqlalchemy import select

from src.models import OutboxEvent
from src.modules.payments.service import PaymentService
from src.worker.outbox_relay import OutboxRelay
from tests.test_payment_service import make_data


class FakeBroker:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[dict] = []

    async def publish(self, message, **kwargs):
        if self.fail:
            raise ConnectionError("rabbitmq is down")
        self.published.append({"message": message, **kwargs})


async def get_events(session_factory) -> list[OutboxEvent]:
    async with session_factory() as session:
        result = await session.execute(select(OutboxEvent))
        return list(result.scalars().all())


async def test_relay_publishes_and_marks_events(session_factory):
    async with session_factory() as session:
        payment, _ = await PaymentService(session).create_payment(make_data(), "key-1")
    broker = FakeBroker()
    relay = OutboxRelay(session_factory, broker)  # type: ignore[arg-type]

    assert await relay.publish_batch() == 1
    assert await relay.publish_batch() == 0

    assert broker.published[0]["routing_key"] == "payments.new"
    assert broker.published[0]["message"]["payment_id"] == str(payment.id)
    [event] = await get_events(session_factory)
    assert event.published_at is not None


async def test_relay_keeps_event_when_broker_is_down(session_factory):
    async with session_factory() as session:
        await PaymentService(session).create_payment(make_data(), "key-1")
    relay = OutboxRelay(session_factory, FakeBroker(fail=True))  # type: ignore[arg-type]

    assert await relay.publish_batch() == 0

    [event] = await get_events(session_factory)
    assert event.published_at is None
    assert event.attempts == 1
    assert event.last_error == "rabbitmq is down"
