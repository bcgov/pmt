# tests/integration/test_order_roundtrip.py

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from messaging.consumer import RedisConsumer
from messaging.outbox.relay import OutboxRelay

pytestmark = pytest.mark.integration


@pytest.fixture
async def running_relay(app_settings, migrated_db, redis_client):
    """A live relay for the duration of one test."""
    relay = OutboxRelay()
    task = asyncio.create_task(relay.start())
    yield relay
    await relay.stop()
    await asyncio.wait_for(task, timeout=10)
    await relay.close()


@pytest.fixture
async def running_consumer(app_settings, migrated_db, redis_client):
    """A live consumer for the duration of one test."""
    consumer = RedisConsumer(consumer_name="roundtrip")
    await consumer.ensure_group()
    task = asyncio.create_task(consumer.start())
    yield consumer
    await consumer.stop()
    await asyncio.wait_for(task, timeout=10)
    await consumer.close()


async def test_order_goes_from_pending_to_confirmed(
    app_settings, running_relay, running_consumer, redis_client, db_session
):
    """
    POST /orders -> order row and outbox row commit together -> relay
    publishes -> consumer confirms -> GET shows confirmed. The whole template
    in one test.
    """
    from main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(
            "/orders",
            json={"order_ref": "rt-1", "item": "widget", "quantity": 4},
        )
        assert created.status_code == 201
        assert created.json()["status"] == "pending"

        status_seen = None
        for _ in range(50):  # up to ~5s
            await asyncio.sleep(0.1)
            response = await client.get("/orders/rt-1")
            assert response.status_code == 200
            status_seen = response.json()["status"]
            if status_seen == "confirmed":
                break

        assert status_seen == "confirmed", "consumer never confirmed the order"
        assert response.json()["confirmed_at"] is not None

    # Nothing failed along the way.
    assert await redis_client.xlen(app_settings.dlq_stream) == 0
