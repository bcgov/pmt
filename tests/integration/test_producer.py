import pytest

from messaging.models import EventEnvelope, OrderCreatedEvent
from messaging.producer.redis_producer import RedisProducer

pytestmark = pytest.mark.integration


async def test_publish_writes_one_message_with_event_field(app_settings, redis_client):
    """
    The field key must be 'event' (singular). The producer and consumer
    disagreed on this before the rewrite, so every message was dropped.
    """
    producer = RedisProducer()
    env = EventEnvelope.create(
        event_type="OrderCreated",
        payload=OrderCreatedEvent(order_ref="r1", item="widget", quantity=1),
        correlation_id="corr-1",
        source="test",
    )

    message_id = await producer.publish(env)
    assert message_id

    entries = await redis_client.xrange(app_settings.STREAM_NAME)
    assert len(entries) == 1
    _, fields = entries[0]
    assert "event" in fields
    assert EventEnvelope.model_validate_json(fields["event"]).payload == env.payload

    await producer.close()
