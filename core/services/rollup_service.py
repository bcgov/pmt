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

    Concurrent writers race on the same key with plain last-write-wins, which
    is NOT always correct: if consumer A reads before consumer B's order
    commits but writes after B's PUT, A's object overwrites B's more-complete
    one and B's order is missing from the rollup. The gap converges on the
    next delivery for that day — the next rebuild recomputes from SQL and
    includes the lost row — but if no later order arrives that day, the lost
    update can persist indefinitely. If a strict guarantee is ever needed,
    take a per-day advisory lock around the SELECT+PUT (e.g.
    `pg_advisory_xact_lock(hash(day))`) or use a conditional PUT with
    `If-Match` and retry on conflict.
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
