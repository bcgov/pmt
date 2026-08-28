import pytest

import cli
from messaging.models import EventEnvelope, OrderCreatedEvent
from messaging.producer.redis_producer import close_producer

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _close_clients(app_settings):
    yield
    await close_producer()


async def test_publish_writes_one_envelope_per_count(app_settings, redis_client):
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

    message_ids = await cli.publish(args)

    assert len(message_ids) == 2
    entries = await redis_client.xrange(app_settings.STREAM_NAME)
    assert len(entries) == 2

    envelope = EventEnvelope.model_validate_json(entries[0][1]["event"])
    assert envelope.event_type == "OrderCreated"
    assert envelope.source == "cli"
    assert envelope.correlation_id == "demo-1"
    assert envelope.traceparent is not None
    assert isinstance(envelope.payload, OrderCreatedEvent)
    assert envelope.payload.unit_price_cents == 450
    assert envelope.payload.quantity == 3


def test_count_defaults_to_one():
    args = cli.build_parser().parse_args(
        [
            "publish",
            "--ref",
            "r",
            "--item",
            "i",
            "--quantity",
            "1",
            "--unit-price-cents",
            "1",
        ]
    )
    assert args.count == 1


def test_negative_unit_price_is_rejected_by_the_parser():
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(
            [
                "publish",
                "--ref",
                "r",
                "--item",
                "i",
                "--quantity",
                "1",
                "--unit-price-cents",
                "-1",
            ]
        )
