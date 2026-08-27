from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging import get_logger
from db.models import Order
from db.repositories.order_repository import OrderRepository
from messaging.models import EventEnvelope, OrderCreatedEvent
from messaging.producer.redis_producer import RedisProducer, get_producer

logger = get_logger(__name__)


class DuplicateOrderError(Exception):
    """Raised when order_ref is already taken."""


class OrderService:
    """
    Business logic for orders.

    Sequencing note: the row is committed BEFORE the event is published.
    The two are not atomic. If the publish fails the order stays `pending`
    and no event exists — the caller surfaces that honestly rather than
    pretending the write failed. A production service would use a
    transactional outbox; see README.
    """

    def __init__(self, session: AsyncSession, producer: RedisProducer | None = None):
        self.session = session
        self.repo = OrderRepository(session)
        self.producer = producer or get_producer()

    async def create_order(
        self, order_ref: str, item: str, quantity: int
    ) -> tuple[Order, str | None]:
        if await self.repo.get_by_ref(order_ref) is not None:
            raise DuplicateOrderError(f"order_ref already exists: {order_ref}")

        order = await self.repo.create(
            order_ref=order_ref, item=item, quantity=quantity
        )
        try:
            await self.session.commit()
        except IntegrityError as e:
            # Two concurrent requests can both pass the check above; the
            # unique constraint is the real guard, this just maps its
            # failure onto the same 409 the explicit check raises.
            await self.session.rollback()
            raise DuplicateOrderError(f"order_ref already exists: {order_ref}") from e
        logger.info("Order created", order_ref=order_ref, status=order.status)

        envelope = EventEnvelope.create(
            event_type="OrderCreated",
            payload=OrderCreatedEvent(
                order_ref=order_ref, item=item, quantity=quantity
            ),
            correlation_id=order_ref,
            source="api",
        )

        try:
            message_id = await self.producer.publish(envelope)
        except Exception as e:
            # The row is committed. Report it as pending rather than lying.
            logger.error(
                "Order committed but event publish failed; order stays pending",
                order_ref=order_ref,
                error=str(e),
                exc_info=True,
            )
            return order, None

        return order, message_id

    async def get_order(self, order_ref: str) -> Order | None:
        return await self.repo.get_by_ref(order_ref)
