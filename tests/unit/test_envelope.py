from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from messaging.models import EventEnvelope, OrderConfirmedEvent, OrderCreatedEvent


def test_create_builds_valid_envelope():
    env = EventEnvelope.create(
        event_type="OrderCreated",
        payload=OrderCreatedEvent(
            order_ref="r1", item="widget", quantity=2, unit_price_cents=450
        ),
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
        payload=OrderCreatedEvent(
            order_ref="r2", item="widget", quantity=1, unit_price_cents=450
        ),
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
            payload=OrderCreatedEvent(
                order_ref="r3", item="w", quantity=1, unit_price_cents=450
            ),
            correlation_id="c",
            source="api",
        )


def test_extra_fields_are_forbidden():
    """A complete, valid envelope plus one unknown key must still be rejected."""
    env = EventEnvelope.create(
        event_type="OrderCreated",
        payload=OrderCreatedEvent(
            order_ref="r4", item="widget", quantity=1, unit_price_cents=450
        ),
        correlation_id="corr-4",
        source="api",
    )
    payload = env.model_dump(mode="json")
    payload["surprise"] = 1

    with pytest.raises(ValidationError):
        EventEnvelope.model_validate(payload)


def make_created():
    return OrderCreatedEvent(
        order_ref="r1", item="widget", quantity=3, unit_price_cents=450
    )


def test_order_confirmed_envelope_round_trips_to_the_right_payload_type():
    """
    EventPayload is a union now. Pydantic must resolve a serialised
    OrderConfirmed back to OrderConfirmedEvent, not coerce it into the first
    member of the union.
    """
    env = EventEnvelope.create(
        event_type="OrderConfirmed",
        payload=OrderConfirmedEvent(
            order_ref="r1", total_cents=1350, confirmed_at=datetime.now(UTC)
        ),
        correlation_id="corr-1",
        source="consumer",
    )

    parsed = EventEnvelope.model_validate_json(env.model_dump_json())

    assert isinstance(parsed.payload, OrderConfirmedEvent)
    assert parsed.payload.total_cents == 1350


def test_totals_survive_the_json_round_trip_as_exact_ints():
    env = EventEnvelope.create(
        event_type="OrderConfirmed",
        payload=OrderConfirmedEvent(
            order_ref="r1", total_cents=100000000000, confirmed_at=datetime.now(UTC)
        ),
        correlation_id="corr-1",
        source="consumer",
    )

    parsed = EventEnvelope.model_validate_json(env.model_dump_json())

    assert parsed.payload.total_cents == 100000000000
    assert isinstance(parsed.payload.total_cents, int)


def test_fractional_unit_price_is_rejected_not_silently_truncated():
    """
    A publisher sending 4.5 means 4.5 cents, which is not representable. It
    must fail validation at the boundary rather than becoming 4 somewhere
    downstream.
    """
    with pytest.raises(ValidationError):
        OrderCreatedEvent(
            order_ref="r1", item="widget", quantity=1, unit_price_cents=4.5
        )


def test_traceparent_is_optional_and_defaults_to_none():
    env = EventEnvelope.create(
        event_type="OrderCreated",
        payload=make_created(),
        correlation_id="corr-1",
        source="cli",
    )

    assert env.traceparent is None
    assert EventEnvelope.model_validate_json(env.model_dump_json()).traceparent is None


def test_traceparent_survives_the_round_trip():
    env = EventEnvelope.create(
        event_type="OrderCreated",
        payload=make_created(),
        correlation_id="corr-1",
        source="cli",
    )
    env = env.model_copy(
        update={
            "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        }
    )

    parsed = EventEnvelope.model_validate_json(env.model_dump_json())

    assert parsed.traceparent == env.traceparent
