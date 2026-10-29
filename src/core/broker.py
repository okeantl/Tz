from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from src.core.config import Settings

PAYMENTS_NEW = "payments.new"
PAYMENTS_DLQ = "payments.new.dlq"

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
dead_letter_exchange = RabbitExchange(
    "payments.dlx", type=ExchangeType.DIRECT, durable=True
)

payments_queue = RabbitQueue(
    PAYMENTS_NEW,
    durable=True,
    routing_key=PAYMENTS_NEW,
    arguments={
        "x-dead-letter-exchange": dead_letter_exchange.name,
        "x-dead-letter-routing-key": PAYMENTS_DLQ,
    },
)
dead_letter_queue = RabbitQueue(PAYMENTS_DLQ, durable=True, routing_key=PAYMENTS_DLQ)


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(settings.RABBITMQ_URL, max_consumers=settings.CONSUMER_PREFETCH)


def retry_delay(attempt: int, base_delay: float) -> float:
    return base_delay * 2 ** (attempt - 1)


def retry_queue_name(attempt: int) -> str:
    return f"{PAYMENTS_NEW}.retry.{attempt}"


def retry_queue(attempt: int, base_delay: float) -> RabbitQueue:
    return RabbitQueue(
        retry_queue_name(attempt),
        durable=True,
        arguments={
            "x-message-ttl": int(retry_delay(attempt, base_delay) * 1000),
            "x-dead-letter-exchange": payments_exchange.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW,
        },
    )


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    exchange = await broker.declare_exchange(payments_exchange)
    dlx = await broker.declare_exchange(dead_letter_exchange)

    queue = await broker.declare_queue(payments_queue)
    await queue.bind(exchange, routing_key=PAYMENTS_NEW)

    dlq = await broker.declare_queue(dead_letter_queue)
    await dlq.bind(dlx, routing_key=PAYMENTS_DLQ)

    for attempt in range(1, settings.MAX_ATTEMPTS):
        await broker.declare_queue(retry_queue(attempt, settings.RETRY_BASE_DELAY))
