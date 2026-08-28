import asyncio

import pytest

import cli
from messaging.consumer.redis_consumer import RedisConsumer
from messaging.models import EventEnvelope, OrderConfirmedEvent
from messaging.producer.redis_producer import close_producer
from messaging.state import close_state_client

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _close_clients(app_settings):
    yield
    await close_state_client()
    await close_producer()


async def drain(consumer: RedisConsumer, passes: int = 2) -> None:
    """Read and handle whatever is pending, `passes` times."""
    for _ in range(passes):
        response = await consumer.redis.xreadgroup(
            groupname=consumer.consumer_group,
            consumername=consumer.consumer_name,
            streams={consumer.stream_name: ">"},
            count=10,
            block=500,
        )
        for _stream, messages in response or []:
            for message_id, fields in messages:
                await consumer._handle_one(message_id, fields)


async def test_cli_publish_flows_through_both_hops(app_settings, redis_client):
    """
    The whole pipeline: CLI publishes OrderCreated twice, the consumer confirms
    once, computes the total, publishes exactly one OrderConfirmed, and handles
    that too. Two events in, one event out.
    """
    args = cli.build_parser().parse_args(
        [
            "publish",
            "--ref",
            "demo-1",
            "--item",
            "widget",
            "--quantity",
            "3",
            "--unit-price-cents",
            "450",
            "--count",
            "2",
        ]
    )
    await cli.publish(args)

    consumer = RedisConsumer()
    await consumer.ensure_group()
    await drain(consumer, passes=3)
    await consumer.close()

    stored = await redis_client.hgetall("order:demo-1")
    assert stored["status"] == "confirmed"
    assert stored["total_cents"] == "1350"

    entries = await redis_client.xrange(app_settings.STREAM_NAME)
    confirmed = [
        EventEnvelope.model_validate_json(fields["event"])
        for _id, fields in entries
        if EventEnvelope.model_validate_json(fields["event"]).event_type
        == "OrderConfirmed"
    ]
    assert len(confirmed) == 1
    assert isinstance(confirmed[0].payload, OrderConfirmedEvent)
    assert confirmed[0].payload.total_cents == 1350

    pending = await redis_client.xpending(
        app_settings.STREAM_NAME, app_settings.CONSUMER_GROUP
    )
    assert pending["pending"] == 0, "every message must be acked"


async def test_nothing_lands_in_the_dlq_on_the_happy_path(app_settings, redis_client):
    args = cli.build_parser().parse_args(
        [
            "publish",
            "--ref",
            "demo-2",
            "--item",
            "widget",
            "--quantity",
            "1",
            "--unit-price-cents",
            "1000",
        ]
    )
    await cli.publish(args)

    consumer = RedisConsumer()
    await consumer.ensure_group()
    await drain(consumer, passes=3)
    await consumer.close()

    assert await redis_client.xlen(app_settings.dlq_stream) == 0
    await asyncio.sleep(0)
