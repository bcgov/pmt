from datetime import UTC, datetime

from messaging.consumer import dispatcher
from messaging.consumer.handlers.order_confirmed import handle
from messaging.models import EventEnvelope, OrderConfirmedEvent


def make_payload():
    return OrderConfirmedEvent(
        order_ref="r1", total_cents=1350, confirmed_at=datetime.now(UTC)
    )


async def test_handle_is_side_effect_free_and_does_not_raise():
    """
    The terminal hop. It must not publish anything — a handler that republished
    onto the same stream would loop the worker forever.
    """
    await handle(make_payload(), correlation_id="corr-1")


async def test_order_confirmed_is_registered_in_the_dispatcher():
    assert "OrderConfirmed" in dispatcher.HANDLERS


async def test_dispatch_routes_an_order_confirmed_envelope_to_its_handler(monkeypatch):
    seen = {}

    async def fake_handle(payload, *, correlation_id):
        seen["total_cents"] = payload.total_cents

    monkeypatch.setitem(dispatcher.HANDLERS, "OrderConfirmed", fake_handle)
    await dispatcher.dispatch_event(
        EventEnvelope.create(
            event_type="OrderConfirmed",
            payload=make_payload(),
            correlation_id="corr-1",
            source="consumer",
        )
    )

    assert seen == {"total_cents": 1350}
