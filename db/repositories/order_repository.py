from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging import get_logger
from db.models import Order

logger = get_logger(__name__)


class OrderRepository:
    """
    Data access for `orders`.

    Methods flush but never commit — transaction boundaries belong to the
    caller (the service for API writes, the handler for consumer writes).
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, order_ref: str, item: str, quantity: int) -> Order:
        order = Order(
            order_ref=order_ref, item=item, quantity=quantity, status="pending"
        )
        self.session.add(order)
        await self.session.flush()
        logger.debug("Order row created", order_ref=order_ref)
        return order

    async def get_by_ref(self, order_ref: str) -> Order | None:
        result = await self.session.execute(
            select(Order).where(Order.order_ref == order_ref)
        )
        return result.scalar_one_or_none()

    async def confirm(self, order_ref: str) -> bool:
        """
        Conditional confirm. Returns False when the order does not exist or
        was already confirmed, which makes replayed events harmless.
        """
        result = await self.session.execute(
            update(Order)
            .where(Order.order_ref == order_ref, Order.status == "pending")
            .values(
                status="confirmed",
                confirmed_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
        return result.rowcount > 0
