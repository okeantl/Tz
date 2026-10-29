import httpx
import pytest

from src.core.broker import retry_delay, retry_queue
from src.main import app

BODY = {
    "amount": "10.00",
    "currency": "USD",
    "description": "Test",
    "webhook_url": "http://example.com/hook",
}


@pytest.fixture
async def client():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
async def test_api_key_required(client, headers):
    response = await client.post(
        "/api/v1/payments", json=BODY, headers={"Idempotency-Key": "k", **headers}
    )
    assert response.status_code == 401


async def test_idempotency_key_required(client):
    response = await client.post(
        "/api/v1/payments", json=BODY, headers={"X-API-Key": "test-api-key"}
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    "field", [{"amount": "0"}, {"amount": "1.001"}, {"currency": "GBP"}]
)
async def test_invalid_body(client, field):
    response = await client.post(
        "/api/v1/payments",
        json=BODY | field,
        headers={"X-API-Key": "test-api-key", "Idempotency-Key": "k"},
    )
    assert response.status_code == 422


def test_retry_queues_have_exponential_ttl():
    assert [retry_delay(attempt, 2) for attempt in (1, 2, 3)] == [2, 4, 8]
    queue = retry_queue(2, 2)
    assert queue.arguments["x-message-ttl"] == 4000
    assert queue.arguments["x-dead-letter-routing-key"] == "payments.new"
