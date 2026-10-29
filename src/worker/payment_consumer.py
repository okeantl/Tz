import asyncio

import httpx
import structlog
from faststream import Context, ContextRepo, FastStream
from faststream.exceptions import RejectMessage
from faststream.rabbit.annotations import RabbitMessage

from src.core.broker import (
    create_broker,
    declare_topology,
    payments_exchange,
    payments_queue,
    retry_delay,
    retry_queue_name,
)
from src.core.config import get_settings
from src.core.database import SessionLocal, engine
from src.core.logging import configure_logging
from src.modules.payments.schemas import PaymentCreatedEvent
from src.worker.processing import (
    PaymentGateway,
    PaymentNotFoundError,
    PaymentProcessor,
    WebhookSender,
)

logger = structlog.get_logger(__name__)

config = get_settings()
broker = create_broker(config)
app = FastStream(broker)


@app.on_startup
async def startup(context: ContextRepo) -> None:
    configure_logging(config.LOG_LEVEL)
    client = httpx.AsyncClient(timeout=config.WEBHOOK_TIMEOUT)
    gateway = PaymentGateway(
        config.PROCESSING_MIN_DELAY,
        config.PROCESSING_MAX_DELAY,
        config.PROCESSING_SUCCESS_RATE,
    )
    context.set_global("http_client", client)
    context.set_global(
        "processor", PaymentProcessor(SessionLocal, gateway, WebhookSender(client))
    )
    await broker.connect()
    await declare_topology(broker, config)


@app.on_shutdown
async def shutdown(context: ContextRepo) -> None:
    await context.get("http_client").aclose()
    await engine.dispose()


@broker.subscriber(payments_queue, payments_exchange)
async def handle_payment(
    event: PaymentCreatedEvent,
    message: RabbitMessage,
    processor: PaymentProcessor = Context(),
) -> None:
    attempt = int(message.headers.get("x-attempt", 1))
    try:
        await processor.process(event.payment_id)
    except PaymentNotFoundError:
        logger.warning("payment_not_found", payment_id=str(event.payment_id))
        raise RejectMessage() from None
    except Exception as e:
        if attempt >= config.MAX_ATTEMPTS:
            logger.error(
                "payment_moved_to_dlq",
                payment_id=str(event.payment_id),
                attempt=attempt,
                error=str(e),
            )
            raise RejectMessage() from e

        delay = retry_delay(attempt, config.RETRY_BASE_DELAY)
        logger.warning(
            "payment_processing_failed",
            payment_id=str(event.payment_id),
            attempt=attempt,
            retry_in=delay,
            error=str(e),
        )
        await broker.publish(
            event.model_dump(mode="json"),
            queue=retry_queue_name(attempt),
            message_id=message.message_id,
            headers={"x-attempt": attempt + 1, "x-last-error": str(e)[:500]},
            persist=True,
        )


if __name__ == "__main__":
    asyncio.run(app.run())
