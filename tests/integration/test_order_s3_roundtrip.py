import asyncio
import json
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient

from messaging.consumer import RedisConsumer
from messaging.outbox.relay import OutboxRelay

pytestmark = pytest.mark.integration

CATALOG = json.dumps(
    {"currency": "USD", "items": {"widget": {"unit_price_cents": 1999}}}
).encode()


@pytest.fixture(autouse=True)
async def _wire_object_store(object_store):
    """
    The consumer's OrderCreated handler prices from S3 through the
    process-wide get_object_store() singleton, not through injection. Point
    it at this test's already-opened store, the same way
    tests/integration/test_order_roundtrip.py and
    tests/integration/test_order_created_handler.py do.
    """
    import storage.s3.client as client_module
    from core.services import pricing as pricing_module

    client_module._store = object_store
    pricing_module._catalog = None
    yield
    pricing_module._catalog = None


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
    consumer = RedisConsumer(consumer_name="s3-roundtrip")
    await consumer.ensure_group()
    task = asyncio.create_task(consumer.start())
    yield consumer
    await consumer.stop()
    await asyncio.wait_for(task, timeout=10)
    await consumer.close()


async def test_an_order_is_priced_from_s3_and_lands_in_the_rollup(
    object_store, running_relay, running_consumer, redis_client, db_session
):
    """
    The whole path: POST -> outbox -> relay -> Redis -> consumer -> S3.

    Mirrors tests/integration/test_order_roundtrip.py, with the two S3
    assertions added.
    """
    from main import app

    await object_store.put("config/prices.json", CATALOG, "application/json")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(
            "/orders",
            json={"order_ref": "s3-1", "item": "widget", "quantity": 3},
        )
        assert created.status_code == 201
        assert created.json()["total_cents"] is None  # not priced yet

        for _ in range(50):
            await asyncio.sleep(0.2)
            fetched = await client.get("/orders/s3-1")
            if fetched.json()["status"] == "confirmed":
                break
        else:
            pytest.fail("order was never confirmed")

    assert fetched.json()["total_cents"] == 5997

    # The order was created moments ago, so its creation date is today's —
    # the handler keys the rollup off created_at, not off the clock.
    key = f"rollups/{datetime.now(UTC).date().isoformat()}.json"
    rollup = await object_store.get(key)
    assert rollup is not None
    document = json.loads(rollup.body)
    assert document["total_cents"] == 5997
    assert [o["order_ref"] for o in document["orders"]] == ["s3-1"]
