import asyncio
import time
from datetime import UTC, datetime, timedelta

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

        self.poll_interval_ms = settings.OUTBOX_POLL_INTERVAL_MS
        self.retention = timedelta(hours=settings.OUTBOX_RETENTION_HOURS)
        self.sweep_interval_s = settings.OUTBOX_SWEEP_INTERVAL_S

        self.running = False
        self.drain_count = 0
        # Built here, not in start(): create_order calls notify() and must not
        # care whether a loop is running in this process.
        self._wake = asyncio.Event()
        self._last_sweep = 0.0

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
        self.drain_count += 1
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

    # ------------------------------------------------------------------
    # Loop
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Drain, sweep, then wait — either for a nudge or for the poll interval,
        whichever comes first.
        """
        self.running = True
        logger.info(
            "Outbox relay started",
            batch_size=self.batch_size,
            poll_interval_ms=self.poll_interval_ms,
        )

        while self.running:
            try:
                claimed = await self.drain_once()
            except Exception as e:
                # A database blip must not end publishing for the life of the
                # process. Log it and keep looping.
                logger.error("Outbox drain failed", error=str(e), exc_info=True)
                claimed = 0

            try:
                await self._maybe_sweep()
            except Exception as e:
                logger.error("Outbox sweep failed", error=str(e), exc_info=True)

            if claimed == 0:
                await self._wait_for_work()

        logger.info("Outbox relay stopped")

    async def _wait_for_work(self) -> None:
        try:
            await asyncio.wait_for(
                self._wake.wait(), timeout=self.poll_interval_ms / 1000
            )
        except TimeoutError:
            pass
        self._wake.clear()

    async def _maybe_sweep(self) -> None:
        now = time.monotonic()
        if now - self._last_sweep < self.sweep_interval_s:
            return
        self._last_sweep = now

        session_maker = self.session_maker
        async with session_maker() as session:
            async with session.begin():
                deleted = await self._repo_factory(session).sweep_published(
                    datetime.now(UTC) - self.retention
                )
        if deleted:
            logger.info("Swept published outbox rows", deleted=deleted)

    def notify(self) -> None:
        """
        Wake the loop now instead of waiting out the poll interval.

        Only reaches a relay in this process, so it is latency, never
        correctness. Safe to call when no loop is running.
        """
        self._wake.set()

    async def stop(self) -> None:
        self.running = False
        self._wake.set()

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.close()


_relay: OutboxRelay | None = None


def get_relay() -> OutboxRelay:
    """
    Process-wide relay, mirroring get_producer(). Constructing it opens
    nothing, so importing this module is free.
    """
    global _relay
    if _relay is None:
        _relay = OutboxRelay()
    return _relay


async def close_relay() -> None:
    """Dispose of the process-wide relay. Called from the app lifespan."""
    global _relay
    if _relay is not None:
        await _relay.stop()
        _relay = None
