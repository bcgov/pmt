from config.logging import get_logger
from db.postgres.session import get_session_maker
from db.repositories.order_repository import OrderRepository
from messaging.models import OrderCreatedEvent

logger = get_logger(__name__)


async def handle(payload: OrderCreatedEvent, *, correlation_id: str) -> None:
    """
    Confirm the order the event refers to.

    SESSION LIFECYCLE — the thing to copy: a handler has no HTTP request, so
    it cannot use Depends(get_db). It opens its own session from the session
    maker, one per message, and commits it. Do not share a session across
    messages; a failure would poison every later message in the batch.

    IDEMPOTENCY: Redis Streams delivers at least once, so this runs again on
    any redelivery. `confirm()` only touches rows still `pending`, which makes
    a replay a logged no-op instead of an error.
    """
    log = logger.bind(order_ref=payload.order_ref, correlation_id=correlation_id)

    session_maker = get_session_maker()
    async with session_maker() as session:
        repo = OrderRepository(session)
        confirmed = await repo.confirm(payload.order_ref)
        await session.commit()

    if confirmed:
        log.info("Order confirmed")
    else:
        log.info("Order already confirmed or missing; nothing to do")
