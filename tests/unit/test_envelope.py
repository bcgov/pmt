import pytest
from pydantic import ValidationError

from messaging.models import EventEnvelope, OrderCreatedEvent


def test_create_builds_valid_envelope():
    env = EventEnvelope.create(
        event_type="OrderCreated",
        payload=OrderCreatedEvent(order_ref="r1", item="widget", quantity=2),
        correlation_id="corr-1",
        source="api",
    )
    assert env.event_type == "OrderCreated"
    assert env.schema_version == "1.0.0"
    assert env.payload.order_ref == "r1"
    assert env.event_id is not None


def test_envelope_round_trips_through_json():
    env = EventEnvelope.create(
        event_type="OrderCreated",
        payload=OrderCreatedEvent(order_ref="r2", item="widget", quantity=1),
        correlation_id="corr-2",
        source="api",
    )
    restored = EventEnvelope.model_validate_json(env.model_dump_json())
    assert restored.payload == env.payload
    assert restored.event_id == env.event_id


def test_unknown_event_type_is_rejected():
    with pytest.raises(ValidationError):
        EventEnvelope.create(
            event_type="Nonsense",
            payload=OrderCreatedEvent(order_ref="r3", item="w", quantity=1),
            correlation_id="c",
            source="api",
        )


def test_extra_fields_are_forbidden():
    """A complete, valid envelope plus one unknown key must still be rejected."""
    env = EventEnvelope.create(
        event_type="OrderCreated",
        payload=OrderCreatedEvent(order_ref="r4", item="widget", quantity=1),
        correlation_id="corr-4",
        source="api",
    )
    payload = env.model_dump(mode="json")
    payload["surprise"] = 1

    with pytest.raises(ValidationError):
        EventEnvelope.model_validate(payload)
