# tests/integration/test_outbox_relay_integration.py

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config.settings import get_settings
from db.models import OutboxEvent
from db.repositories.outbox_repository import OutboxRepository
from messaging.models import EventEnvelope, OrderCreatedEvent
from messaging.outbox.relay import OutboxRelay
from messaging.producer.redis_producer import RedisProducer

pytestmark = pytest.mark.integration


def make_envelope(order_ref: str) -> EventEnvelope:
    return EventEnvelope.create(
        event_type="OrderCreated",
        payload=OrderCreatedEvent(order_ref=order_ref, item="widget", quantity=1),
        correlation_id=order_ref,
        source="api",
    )


@pytest.fixture
async def session_maker(migrated_db):
    engine = create_async_engine(get_settings().DATABASE_URL)
    yield async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    await engine.dispose()


async def test_pending_row_is_published_byte_for_byte_and_marked(
    db_session, redis_client, session_maker, app_settings
):
    envelope = make_envelope("r1")
    row = await OutboxRepository(db_session).add(envelope)
    await db_session.commit()
    stored = row.payload

    producer = RedisProducer()
    relay = OutboxRelay(producer=producer, session_maker=session_maker)
    claimed = await relay.drain_once()
    await producer.close()

    assert claimed == 1
    entries = await redis_client.xrange(app_settings.STREAM_NAME)
    assert len(entries) == 1
    assert entries[0][1]["event"] == stored
    assert json.loads(entries[0][1]["event"])["correlation_id"] == "r1"

    await db_session.refresh(row)
    assert row.status == "published"
    assert row.published_at is not None


async def test_redis_down_leaves_the_row_pending_then_publishes_on_recovery(
    db_session, redis_client, session_maker, app_settings
):
    row = await OutboxRepository(db_session).add(make_envelope("r1"))
    await db_session.commit()

    class DownProducer:
        async def publish_raw(self, payload):
            raise RedisConnectionError("connection refused")

    relay = OutboxRelay(producer=DownProducer(), session_maker=session_maker)
    await relay.drain_once()

    await db_session.refresh(row)
    assert row.status == "pending"
    assert row.attempts == 1
    assert "connection refused" in row.last_error

    # The backoff parks it in the future; rewind so the retry is claimable.
    row.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
    await db_session.commit()

    producer = RedisProducer()
    await OutboxRelay(producer=producer, session_maker=session_maker).drain_once()
    await producer.close()

    await db_session.refresh(row)
    assert row.status == "published"
    assert await redis_client.xlen(app_settings.STREAM_NAME) == 1


async def test_a_corrupt_row_is_failed_and_the_rest_of_the_batch_drains(
    db_session, redis_client, session_maker, app_settings
):
    """
    Corruption is the only way a committed row can be unpublishable, and it
    must not block the rows behind it.
    """
    repo = OutboxRepository(db_session)
    bad = await repo.add(make_envelope("bad"))
    good = await repo.add(make_envelope("good"))
    bad.payload = ""
    await db_session.commit()

    producer = RedisProducer()
    await OutboxRelay(producer=producer, session_maker=session_maker).drain_once()
    await producer.close()

    await db_session.refresh(bad)
    await db_session.refresh(good)
    assert bad.status == "failed"
    assert bad.failed_at is not None
    assert good.status == "published"
    assert await redis_client.xlen(app_settings.STREAM_NAME) == 1


async def test_failed_rows_are_not_published_to_the_dlq_stream(
    db_session, redis_client, session_maker, app_settings
):
    """
    The failed row IS the dead letter. Marking it must not depend on a Redis
    write, since Redis is what may have failed.
    """
    row = await OutboxRepository(db_session).add(make_envelope("bad"))
    row.payload = ""
    await db_session.commit()

    producer = RedisProducer()
    await OutboxRelay(producer=producer, session_maker=session_maker).drain_once()
    await producer.close()

    assert await redis_client.xlen(app_settings.dlq_stream) == 0


async def test_two_concurrent_relays_publish_every_row_once(
    db_session, redis_client, session_maker, app_settings
):
    """
    Demonstrates non-overlapping claims under SKIP LOCKED, not exactly-once
    delivery: the architecture is at-least-once, and a crash between XADD and
    the batch commit still republishes.
    """
    repo = OutboxRepository(db_session)
    for ref in ("r1", "r2", "r3", "r4", "r5", "r6"):
        await repo.add(make_envelope(ref))
    await db_session.commit()

    producers = [RedisProducer(), RedisProducer()]
    relays = [OutboxRelay(producer=p, session_maker=session_maker) for p in producers]
    for relay in relays:
        relay.batch_size = 3

    await asyncio.gather(*(relay.drain_once() for relay in relays))
    for p in producers:
        await p.close()

    entries = await redis_client.xrange(app_settings.STREAM_NAME)
    refs = [json.loads(e[1]["event"])["correlation_id"] for e in entries]
    assert sorted(refs) == ["r1", "r2", "r3", "r4", "r5", "r6"]

    statuses = (await db_session.execute(select(OutboxEvent.status))).scalars().all()
    assert set(statuses) == {"published"}


async def test_the_sweep_removes_aged_published_rows_only(
    db_session, session_maker, app_settings
):
    repo = OutboxRepository(db_session)
    old = await repo.add(make_envelope("old"))
    failed = await repo.add(make_envelope("failed"))
    pending = await repo.add(make_envelope("pending"))
    await repo.mark_published(old)
    await repo.mark_failed(failed, "boom")
    old.published_at = datetime.now(UTC) - timedelta(days=30)
    await db_session.commit()

    relay = OutboxRelay(producer=RedisProducer(), session_maker=session_maker)
    relay.retention = timedelta(hours=1)
    relay._last_sweep = 0.0
    await relay._maybe_sweep()
    await relay.close()

    remaining = (
        (await db_session.execute(select(OutboxEvent.correlation_id))).scalars().all()
    )
    assert sorted(remaining) == ["failed", "pending"]
    assert pending.status == "pending"
