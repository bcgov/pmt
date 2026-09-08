from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging import get_logger
from db.models import OutboxEvent
from messaging.models import EventEnvelope

logger = get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class OutboxRepository:
    """
    Data access for `outbox`.

    Like OrderRepository: flushes, never commits. The writer's transaction is
    the whole point of this table, so the boundary belongs to the caller.
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, envelope: EventEnvelope) -> OutboxEvent:
        """
        Append one event. The envelope is serialized exactly once, here, and
        that string is what eventually reaches Redis untouched.
        """
        row = OutboxEvent(
            event_id=envelope.event_id,
            event_type=envelope.event_type,
            correlation_id=envelope.correlation_id,
            source=envelope.source,
            payload=envelope.model_dump_json(),
        )
        self.session.add(row)
        await self.session.flush()
        logger.debug(
            "Outbox row written",
            event_id=str(envelope.event_id),
            event_type=envelope.event_type,
        )
        return row

    async def claim_batch(self, limit: int) -> list[OutboxEvent]:
        """
        Lock and return the next publishable rows.

        SKIP LOCKED is what makes several relay replicas safe: each skips
        whatever another already holds instead of blocking behind it.
        """
        result = await self.session.execute(
            select(OutboxEvent)
            .where(
                OutboxEvent.status == "pending",
                OutboxEvent.next_attempt_at <= _utcnow(),
            )
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.scalars().all())

    async def mark_published(self, row: OutboxEvent) -> None:
        row.status = "published"
        row.published_at = _utcnow()
        await self.session.flush()

    async def mark_failed(self, row: OutboxEvent, error: str) -> None:
        """
        Terminal failure. The row itself is the dead letter: it keeps the full
        payload, the error and the timestamps, and is queryable with SELECT.
        Nothing is published to Redis to record this — the SQL row is
        authoritative precisely because Redis may be what failed.
        """
        row.status = "failed"
        row.failed_at = _utcnow()
        row.last_error = error
        await self.session.flush()

    async def mark_retry(self, row: OutboxEvent, error: str, backoff_ms: int) -> None:
        row.attempts += 1
        row.last_error = error
        row.next_attempt_at = _utcnow() + timedelta(milliseconds=backoff_ms)
        await self.session.flush()

    async def sweep_published(self, older_than: datetime) -> int:
        """
        Retention. Only `published` rows are swept — `failed` rows are the
        dead-letter record and are removed by an operator after investigation.
        """
        result = await self.session.execute(
            delete(OutboxEvent).where(
                OutboxEvent.status == "published",
                OutboxEvent.published_at < older_than,
            )
        )
        return result.rowcount
