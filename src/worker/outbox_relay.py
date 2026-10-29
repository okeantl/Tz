import asyncio
import contextlib
import signal
from datetime import UTC, datetime

import structlog
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.broker import create_broker, declare_topology, payments_exchange
from src.core.config import get_settings
from src.core.database import SessionLocal, engine
from src.core.logging import configure_logging
from src.models import OutboxEvent

logger = structlog.get_logger(__name__)


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        batch_size: int = 100,
    ) -> None:
        self.session_factory = session_factory
        self.broker = broker
        self.batch_size = batch_size

    async def publish_batch(self) -> int:
        published = 0
        async with self.session_factory() as db, db.begin():
            result = await db.execute(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(self.batch_size)
                .with_for_update(skip_locked=True)
            )
            for event in result.scalars().all():
                event.attempts += 1
                try:
                    await self.broker.publish(
                        event.payload,
                        exchange=payments_exchange,
                        routing_key=event.event_type,
                        message_id=str(event.id),
                        headers={"x-attempt": 1},
                        persist=True,
                    )
                except Exception as e:
                    event.last_error = str(e)[:1000]
                    logger.warning(
                        "outbox_publish_failed", event_id=str(event.id), error=str(e)
                    )
                    break
                event.published_at = datetime.now(UTC)
                published += 1
        if published:
            logger.info("outbox_published", count=published)
        return published


async def main() -> None:
    config = get_settings()
    configure_logging(config.LOG_LEVEL)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    broker = create_broker(config)
    async with broker:
        await declare_topology(broker, config)
        relay = OutboxRelay(SessionLocal, broker, config.OUTBOX_BATCH_SIZE)
        logger.info("outbox_relay_started")
        while not stop.is_set():
            try:
                published = await relay.publish_batch()
            except Exception as e:
                logger.error("outbox_relay_error", error=str(e))
                published = 0
            if published < relay.batch_size:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), config.OUTBOX_POLL_INTERVAL)
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
