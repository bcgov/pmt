from config.logging import get_logger
from config.settings import get_settings
from db.postgres.session import get_session_maker
from db.repositories.outbox_repository import OutboxRepository
from messaging.outbox.backoff import backoff_ms, is_retryable
from messaging.producer.redis_producer import RedisProducer, get_producer

logger = get_logger(__name__)


class OutboxRelay:
    """
    Publishes rows the writer committed to `outbox`.

    The relay is the service's only XADD call site for domain events. It never
    parses `payload`: the writer serialized a validated envelope, and the relay
    moves that exact string from Postgres to Redis.

    One transaction covers a whole batch — the claim, every XADD and every
    mark. That keeps claim-and-mark atomic at the cost of a wider duplicate
    window: a crash mid-batch rolls back every mark and republishes rows that
    already reached Redis. OUTBOX_BATCH_SIZE bounds it.
    """

    def __init__(
        self,
        producer: RedisProducer | None = None,
        session_maker=None,
    ) -> None:
        settings = get_settings()

        # Injection seams for tests. Constructing a relay must not open a
        # socket or a connection, so both are resolved lazily in production.
        self._producer = producer
        self._session_maker = session_maker
        self._repo_factory = OutboxRepository

        self.batch_size = settings.OUTBOX_BATCH_SIZE
        self.retry_backoff_ms = settings.OUTBOX_RETRY_BACKOFF_MS
        self.max_backoff_ms = settings.OUTBOX_MAX_BACKOFF_MS

    @property
    def producer(self) -> RedisProducer:
        return self._producer or get_producer()

    @property
    def session_maker(self):
        return self._session_maker or get_session_maker()

    async def drain_once(self) -> int:
        """
        Claim one batch and publish it. Returns the number of rows claimed —
        zero means the loop should go back to waiting.
        """
        session_maker = self.session_maker
        async with session_maker() as session:
            async with session.begin():
                repo = self._repo_factory(session)
                rows = await repo.claim_batch(self.batch_size)

                for row in rows:
                    if not row.payload:
                        # Only reachable through corruption; surface it as a
                        # failed row rather than crashing the relay.
                        await repo.mark_failed(row, "empty payload")
                        logger.error("Outbox row has no payload", outbox_id=row.id)
                        continue

                    try:
                        await self.producer.publish_raw(row.payload)
                    except Exception as exc:
                        if is_retryable(exc):
                            wait = backoff_ms(
                                row.attempts,
                                base_ms=self.retry_backoff_ms,
                                cap_ms=self.max_backoff_ms,
                            )
                            await repo.mark_retry(row, str(exc), wait)
                            logger.warning(
                                "Outbox publish failed; will retry",
                                outbox_id=row.id,
                                attempts=row.attempts,
                                backoff_ms=wait,
                                error=str(exc),
                            )
                            # Redis is unreachable for every row, not just
                            # this one. Stop, and keep publish order.
                            break

                        await repo.mark_failed(row, str(exc))
                        logger.error(
                            "Outbox publish failed permanently; row dead-lettered",
                            outbox_id=row.id,
                            error=str(exc),
                            exc_info=True,
                        )
                        # One unpublishable row must not block the rest.
                        continue

                    await repo.mark_published(row)

                return len(rows)
