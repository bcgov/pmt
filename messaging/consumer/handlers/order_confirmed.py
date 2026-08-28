from config.logging import get_logger
from messaging.models import OrderConfirmedEvent
from money import format_cents

logger = get_logger(__name__)


async def handle(payload: OrderConfirmedEvent, *, correlation_id: str) -> None:
    """
    The terminal hop of the sample pipeline.

    Deliberately side-effect-free: it exists so the consumer-as-producer step
    in the OrderCreated handler has a real consumer, which is what makes the
    distributed trace span two process hops instead of one.

    It has no idempotency guard because it has no state to guard — see
    handlers/order_created.py for the HSETNX pattern to copy when your handler
    does write something.

    DO NOT publish OrderCreated from here. Both event types share one stream,
    so a handler that republishes upstream gives you an infinite loop that
    looks exactly like a busy worker.
    """
    logger.bind(order_ref=payload.order_ref, correlation_id=correlation_id).info(
        "Order confirmation received",
        total=format_cents(payload.total_cents),
        confirmed_at=payload.confirmed_at.isoformat(),
    )
