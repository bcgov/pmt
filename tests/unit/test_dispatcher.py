import pytest

from messaging.consumer import dispatcher
from messaging.models import EventEnvelope, OrderCreatedEvent


def make_envelope(event_type="OrderCreated"):
    return EventEnvelope.create(
        event_type=event_type,
        payload=OrderCreatedEvent(
            order_ref="r1", item="widget", quantity=1, unit_price_cents=450
        ),
        correlation_id="corr-1",
        source="test",
    )


async def test_dispatch_calls_the_registered_handler(monkeypatch):
    seen = {}

    async def fake_handle(payload, *, correlation_id):
        seen["order_ref"] = payload.order_ref
        seen["correlation_id"] = correlation_id

    monkeypatch.setitem(dispatcher.HANDLERS, "OrderCreated", fake_handle)
    await dispatcher.dispatch_event(make_envelope())

    assert seen == {"order_ref": "r1", "correlation_id": "corr-1"}


async def test_unknown_event_type_is_ignored_not_raised(monkeypatch):
    """
    A consumer group receives every event on the stream. Not caring about a
    sibling service's event type is normal, so this must not raise (the
    consumer acks it).
    """
    envelope = make_envelope()
    monkeypatch.setattr(dispatcher, "HANDLERS", {})
    await dispatcher.dispatch_event(envelope)  # must not raise


async def test_handler_exceptions_propagate(monkeypatch):
    """The consumer needs the exception to trigger retry and dead-lettering."""

    async def boom(payload, *, correlation_id):
        raise RuntimeError("handler exploded")

    monkeypatch.setitem(dispatcher.HANDLERS, "OrderCreated", boom)
    with pytest.raises(RuntimeError, match="handler exploded"):
        await dispatcher.dispatch_event(make_envelope())
