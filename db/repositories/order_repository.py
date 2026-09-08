from datetime import UTC, date, datetime, time, timedelta

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

    async def confirm(self, order_ref: str, total_cents: int | None = None) -> bool:
        """
        Conditional confirm. Returns False when the order does not exist or
        was already confirmed, which makes replayed events harmless.

        The total is written in the same UPDATE, so an order can never be
        `confirmed` with no price — and a replay cannot overwrite the price
        the first delivery computed, because the WHERE clause excludes it.
        """
        result = await self.session.execute(
            update(Order)
            .where(Order.order_ref == order_ref, Order.status == "pending")
            .values(
                status="confirmed",
                total_cents=total_cents,
                confirmed_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
        return result.rowcount > 0

    async def list_confirmed_created_on(self, day: date) -> list[Order]:
        """
        Confirmed orders *created* on `day` (UTC), oldest confirmation first.

        Created, not confirmed: an order placed at 23:59 and confirmed at
        00:01 must stay in the rollup for the day it was placed, or a rebuild
        would drop it from one file without adding it to another.
        """
        start = datetime.combine(day, time.min, tzinfo=UTC)
        end = start + timedelta(days=1)
        result = await self.session.execute(
            select(Order)
            .where(
                Order.status == "confirmed",
                Order.created_at >= start,
                Order.created_at < end,
            )
            .order_by(Order.confirmed_at)
        )
        return list(result.scalars().all())
