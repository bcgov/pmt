import json
from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from config.logging import get_logger
from config.settings import get_settings
from db.repositories.order_repository import OrderRepository
from storage.object_store import ObjectStore

logger = get_logger(__name__)


class RollupService:
    """
    Writes the daily rollup object.

    The object is a projection, not an accumulator: this rebuilds the whole
    day from SQL and overwrites the key. It never reads what is there.

    That is what makes concurrency a non-problem. Several consumers can race
    on the same key; the last write wins, and the winner is correct because
    every writer computed from the same authoritative rows. Merging into the
    existing object instead would need a conditional PUT and a retry loop to
    avoid losing updates.
    """

    def __init__(self, session: AsyncSession, store: ObjectStore) -> None:
        self.session = session
        self.store = store
        self.repo = OrderRepository(session) if session is not None else None

    async def rebuild_for_date(self, day: date, *, currency: str = "USD") -> str:
        orders = await self.repo.list_confirmed_created_on(day)
        rows = [
            {
                "order_ref": order.order_ref,
                "item": order.item,
                "quantity": order.quantity,
                # Derived, not stored: the catalog price at confirmation time
                # is whatever the total implies, which keeps the object
                # consistent with the row even if the catalog changes later.
                "unit_price_cents": order.total_cents // order.quantity,
                "total_cents": order.total_cents,
            }
            for order in orders
            if order.total_cents is not None and order.quantity
        ]
        document = {
            "date": day.isoformat(),
            "currency": currency,
            "order_count": len(rows),
            "total_cents": sum(row["total_cents"] for row in rows),
            "orders": rows,
        }

        key = get_settings().rollup_key(day)
        await self.store.put(
            key,
            json.dumps(document, separators=(",", ":")).encode(),
            "application/json",
        )
        logger.info(
            "Rollup rebuilt",
            key=key,
            order_count=document["order_count"],
            total_cents=document["total_cents"],
        )
        return key
