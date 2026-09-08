from config.logging import get_logger
from core.services.pricing import get_price_catalog
from core.services.rollup_service import RollupService
from db.postgres.session import get_session_maker
from db.repositories.order_repository import OrderRepository
from messaging.models import OrderCreatedEvent
from storage.errors import PermanentHandlerError
from storage.s3.client import get_object_store

logger = get_logger(__name__)


async def handle(payload: OrderCreatedEvent, *, correlation_id: str) -> None:
    """
    Price the order from S3, confirm it, and rebuild the day's rollup object.

    SESSION LIFECYCLE — the thing to copy: a handler has no HTTP request, so
    it cannot use Depends(get_db). It opens its own session from the session
    maker, one per message, and commits it. Do not share a session across
    messages; a failure would poison every later message in the batch.

    IDEMPOTENCY: Redis Streams delivers at least once, so this runs again on
    any redelivery. `confirm()` only touches rows still `pending`, which makes
    a replay a logged no-op instead of an error — and cannot overwrite the
    total the first delivery computed.

    ORDERING: SQL commits before the object is written, and the rollup is
    rebuilt on EVERY delivery, including one whose confirm() was a no-op.
    Skipping the rebuild on a redelivery would look like an optimization and
    would in fact break recovery: if the PUT failed after a successful commit,
    the retry's confirm() affects no rows, and the object would never be
    written at all. Rebuilding unconditionally makes a retry converge.
    """
    log = logger.bind(order_ref=payload.order_ref, correlation_id=correlation_id)

    catalog = await get_price_catalog().get()
    unit_price_cents = catalog.unit_price_cents(payload.item)
    total_cents = unit_price_cents * payload.quantity

    session_maker = get_session_maker()
    async with session_maker() as session:
        repo = OrderRepository(session)
        confirmed = await repo.confirm(payload.order_ref, total_cents=total_cents)
        await session.commit()

        if confirmed:
            log.info("Order confirmed", total_cents=total_cents)
        else:
            log.info("Order already confirmed or missing; rebuilding rollup anyway")

        # The rollup day comes from the ORDER's creation date, not from the
        # clock. A message consumed at 00:01 for an order placed at 23:59
        # belongs to yesterday's file; using today's date would rebuild the
        # wrong object and leave the right one missing that order forever.
        order = await repo.get_by_ref(payload.order_ref)
        if order is None:
            raise PermanentHandlerError(f"no such order: {payload.order_ref}")

        rollups = RollupService(session, get_object_store())
        await rollups.rebuild_for_date(
            order.created_at.date(), currency=catalog.currency
        )
