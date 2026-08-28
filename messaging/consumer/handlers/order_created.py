from datetime import UTC, datetime

from config.logging import get_logger
from config.settings import get_settings
from messaging.models import EventEnvelope, OrderConfirmedEvent, OrderCreatedEvent
from messaging.producer.redis_producer import get_producer
from messaging.state import get_state_client
from money import format_cents

logger = get_logger(__name__)


async def handle(payload: OrderCreatedEvent, *, correlation_id: str) -> None:
    """
    Consume, compute, publish — the shape every handler in this template takes.

    IDEMPOTENCY: Redis Streams delivers at least once, so this runs again on
    any redelivery. HSETNX is the guard: it writes only if the field is absent,
    so exactly one delivery per order_ref takes the work path. Note where the
    early return sits — BEFORE the publish. Correct local state is only half of
    idempotency; the other half is not amplifying a redelivery into a duplicate
    downstream event.

    NON-ATOMICITY, on purpose: HSETNX and EXPIRE are two round trips, so a
    crash between them leaves a key with no TTL. A Lua script or a SET NX with
    an embedded TTL would be atomic. For a demo state store the simpler code is
    worth more than the guarantee — but do not copy this shape into a place
    where the TTL is load-bearing.

    RESOURCES: the handler takes its own Redis client (messaging/state.py)
    rather than the consumer's. There is no request scope here to inherit one
    from.

    FAILURE WINDOW, documented rather than solved: if HSETNX succeeds and the
    publish below fails, this raises and the message is retried — but the retry
    finds HSETNX returning 0 and takes the early return, so OrderConfirmed is
    never published. The state is right and the downstream event is lost. That
    is the write-then-publish problem every service with a database and a
    broker has; the real answer is a transactional outbox, which is more
    machinery than a template should carry. Know that it is here before you
    copy this into something that matters.
    """
    log = logger.bind(order_ref=payload.order_ref, correlation_id=correlation_id)

    redis = get_state_client()
    key = f"order:{payload.order_ref}"

    if not await redis.hsetnx(key, "status", "confirmed"):
        log.info("Order already processed; nothing to do")
        return

    # The processing step. Integer arithmetic on minor units, exact by
    # construction. Replace this with whatever your service actually does.
    total_cents = payload.quantity * payload.unit_price_cents
    confirmed_at = datetime.now(UTC)

    await redis.hset(
        key,
        mapping={
            "item": payload.item,
            "quantity": payload.quantity,
            "total_cents": total_cents,
            "confirmed_at": confirmed_at.isoformat(),
        },
    )
    await redis.expire(key, get_settings().STATE_TTL_SECONDS)

    # Publishing from inside a handler is what makes this service a node in a
    # pipeline rather than a leaf. The producer reads ambient trace context, so
    # this event is automatically a child of the span for the message being
    # handled — see messaging/consumer/redis_consumer.py.
    await get_producer().publish(
        EventEnvelope.create(
            event_type="OrderConfirmed",
            payload=OrderConfirmedEvent(
                order_ref=payload.order_ref,
                total_cents=total_cents,
                confirmed_at=confirmed_at,
            ),
            correlation_id=correlation_id,
            source="consumer",
        )
    )

    log.info("Order confirmed", total=format_cents(total_cents))
