from collections.abc import Awaitable, Callable

from config.logging import get_logger
from messaging.consumer.handlers.order_created import handle as handle_order_created
from messaging.models import EventEnvelope, EventPayload

logger = get_logger(__name__)

Handler = Callable[..., Awaitable[None]]

# Event routing table. Register new event types here.
HANDLERS: dict[str, Handler] = {
    "OrderCreated": handle_order_created,
}


async def dispatch_event(envelope: EventEnvelope) -> None:
    """
    Route a validated envelope to its handler.

    `EventEnvelope.event_type` is a strict `Literal`, so any event type not
    already known fails envelope validation before dispatch is ever called —
    this branch is not reachable via a sibling service's events on a shared
    stream. It guards against a HANDLERS registry gap: a type added to the
    Literal (see envelope.py) without a matching handler registered here.
    Handler exceptions DO propagate — the consumer needs them to drive retry
    and dead-lettering.
    """
    handler: Handler | None = HANDLERS.get(envelope.event_type)

    if handler is None:
        logger.warning(
            "No handler registered for event type; acking",
            event_type=envelope.event_type,
            correlation_id=envelope.correlation_id,
        )
        return

    logger.debug(
        "Dispatching event",
        event_type=envelope.event_type,
        correlation_id=envelope.correlation_id,
    )

    payload: EventPayload = envelope.payload
    await handler(payload, correlation_id=envelope.correlation_id)
