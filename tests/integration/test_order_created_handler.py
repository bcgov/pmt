import pytest

from messaging.consumer.handlers.order_created import handle
from messaging.models import EventEnvelope, OrderConfirmedEvent, OrderCreatedEvent
from messaging.producer.redis_producer import close_producer
from messaging.state import close_state_client

pytestmark = pytest.mark.integration


def make_payload(ref="r1", quantity=3, unit_price_cents=450):
    return OrderCreatedEvent(
        order_ref=ref,
        item="widget",
        quantity=quantity,
        unit_price_cents=unit_price_cents,
    )


@pytest.fixture(autouse=True)
async def _close_clients(app_settings):
    """
    The handler's module-level clients bind to the event loop that created
    them, and pytest-asyncio gives each test its own loop. Close them after
    every test or the second test to run fails with "Event loop is closed".
    """
    yield
    await close_state_client()
    await close_producer()


async def test_first_delivery_confirms_computes_and_publishes(
    app_settings, redis_client
):
    await handle(make_payload(), correlation_id="corr-1")

    stored = await redis_client.hgetall("order:r1")
    assert stored["status"] == "confirmed"
    assert stored["item"] == "widget"
    assert stored["quantity"] == "3"
    assert stored["total_cents"] == "1350"
    assert stored["confirmed_at"]

    ttl = await redis_client.ttl("order:r1")
    assert 0 < ttl <= app_settings.STATE_TTL_SECONDS

    entries = await redis_client.xrange(app_settings.STREAM_NAME)
    assert len(entries) == 1
    envelope = EventEnvelope.model_validate_json(entries[0][1]["event"])
    assert envelope.event_type == "OrderConfirmed"
    assert envelope.source == "consumer"
    assert envelope.correlation_id == "corr-1"
    assert isinstance(envelope.payload, OrderConfirmedEvent)
    assert envelope.payload.total_cents == 1350


async def test_redelivery_is_a_no_op_and_publishes_nothing(app_settings, redis_client):
    """
    The whole point. Redis Streams delivers at least once, so this handler runs
    again on redelivery. Local state staying correct is not enough — a
    duplicate inbound event must not become a duplicate outbound event, or one
    redelivery amplifies through every downstream consumer.
    """
    await handle(make_payload(), correlation_id="corr-1")
    await handle(make_payload(), correlation_id="corr-1")

    entries = await redis_client.xrange(app_settings.STREAM_NAME)
    assert len(entries) == 1, "redelivery must not publish a second OrderConfirmed"


async def test_redelivery_does_not_overwrite_the_stored_total(
    app_settings, redis_client
):
    await handle(make_payload(quantity=3, unit_price_cents=450), correlation_id="c")
    await handle(make_payload(quantity=9, unit_price_cents=999), correlation_id="c")

    stored = await redis_client.hgetall("order:r1")
    assert stored["total_cents"] == "1350"


async def test_total_is_exact_for_large_amounts(app_settings, redis_client):
    await handle(
        make_payload(quantity=3, unit_price_cents=333_333_333), correlation_id="c"
    )

    stored = await redis_client.hgetall("order:r1")
    assert stored["total_cents"] == "999999999"
