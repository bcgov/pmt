from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging import get_logger
from db.models import Order
from db.repositories.order_repository import OrderRepository
from db.repositories.outbox_repository import OutboxRepository
from messaging.models import EventEnvelope, OrderCreatedEvent
from messaging.outbox.relay import get_relay

logger = get_logger(__name__)


class DuplicateOrderError(Exception):
    """Raised when order_ref is already taken."""


class OrderService:
    """
    Business logic for orders.

    Sequencing note: the order row and its event are written in ONE
    transaction — the event goes to the `outbox` table, not to Redis. This
    method never publishes. The relay does that, after the commit, which is
    what makes "the row exists but the event does not" impossible.
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self.repo = OrderRepository(session)
        self.outbox = OutboxRepository(session)

    async def create_order(self, order_ref: str, item: str, quantity: int) -> Order:
        if await self.repo.get_by_ref(order_ref) is not None:
            raise DuplicateOrderError(f"order_ref already exists: {order_ref}")

        order = await self.repo.create(
            order_ref=order_ref, item=item, quantity=quantity
        )

        envelope = EventEnvelope.create(
            event_type="OrderCreated",
            payload=OrderCreatedEvent(
                order_ref=order_ref, item=item, quantity=quantity
            ),
            correlation_id=order_ref,
            source="api",
        )
        await self.outbox.add(envelope)

        try:
            await self.session.commit()
        except IntegrityError as e:
            # Two concurrent requests can both pass the check above; the
            # unique constraint is the real guard, this just maps its
            # failure onto the same 409 the explicit check raises. The outbox
            # row rolls back with the order row — that is the point.
            await self.session.rollback()
            raise DuplicateOrderError(f"order_ref already exists: {order_ref}") from e

        logger.info("Order created", order_ref=order_ref, status=order.status)

        # Latency only: wakes a relay in this process so the event does not
        # wait out the poll interval. Harmless when RELAY_ENABLED is false.
        get_relay().notify()

        return order

    async def get_order(self, order_ref: str) -> Order | None:
        return await self.repo.get_by_ref(order_ref)
