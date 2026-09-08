# tests/integration/test_outbox_repository.py

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config.settings import get_settings
from db.models import OutboxEvent
from db.repositories.outbox_repository import OutboxRepository
from messaging.models import EventEnvelope, OrderCreatedEvent

pytestmark = pytest.mark.integration


def make_envelope(order_ref: str) -> EventEnvelope:
    return EventEnvelope.create(
        event_type="OrderCreated",
        payload=OrderCreatedEvent(order_ref=order_ref, item="widget", quantity=1),
        correlation_id=order_ref,
        source="api",
    )


async def test_add_stores_the_serialized_envelope_and_lifts_out_columns(db_session):
    envelope = make_envelope("r1")

    row = await OutboxRepository(db_session).add(envelope)
    await db_session.commit()

    assert row.payload == envelope.model_dump_json()
    assert row.event_id == envelope.event_id
    assert row.event_type == "OrderCreated"
    assert row.correlation_id == "r1"
    assert row.source == "api"
    assert row.status == "pending"


async def test_claim_batch_returns_pending_rows_in_id_order(db_session):
    repo = OutboxRepository(db_session)
    for ref in ("r1", "r2", "r3"):
        await repo.add(make_envelope(ref))
    await db_session.commit()

    rows = await repo.claim_batch(limit=10)

    assert [r.correlation_id for r in rows] == ["r1", "r2", "r3"]


async def test_claim_batch_respects_the_limit(db_session):
    repo = OutboxRepository(db_session)
    for ref in ("r1", "r2", "r3"):
        await repo.add(make_envelope(ref))
    await db_session.commit()

    assert len(await repo.claim_batch(limit=2)) == 2


async def test_claim_batch_skips_rows_whose_backoff_has_not_elapsed(db_session):
    repo = OutboxRepository(db_session)
    row = await repo.add(make_envelope("r1"))
    row.next_attempt_at = datetime.now(UTC) + timedelta(minutes=5)
    await db_session.commit()

    assert await repo.claim_batch(limit=10) == []


async def test_claim_batch_ignores_published_and_failed_rows(db_session):
    repo = OutboxRepository(db_session)
    published = await repo.add(make_envelope("r1"))
    failed = await repo.add(make_envelope("r2"))
    await repo.mark_published(published)
    await repo.mark_failed(failed, "boom")
    await db_session.commit()

    assert await repo.claim_batch(limit=10) == []


async def test_mark_published_sets_the_terminal_state(db_session):
    repo = OutboxRepository(db_session)
    row = await repo.add(make_envelope("r1"))

    await repo.mark_published(row)
    await db_session.commit()

    assert row.status == "published"
    assert row.published_at is not None
    assert row.failed_at is None


async def test_mark_failed_records_the_error_and_time(db_session):
    repo = OutboxRepository(db_session)
    row = await repo.add(make_envelope("r1"))

    await repo.mark_failed(row, "WRONGTYPE")
    await db_session.commit()

    assert row.status == "failed"
    assert row.failed_at is not None
    assert row.last_error == "WRONGTYPE"
    assert row.published_at is None


async def test_mark_retry_leaves_the_row_pending_and_pushes_it_out(db_session):
    repo = OutboxRepository(db_session)
    row = await repo.add(make_envelope("r1"))
    before = row.next_attempt_at

    await repo.mark_retry(row, "redis is down", backoff_ms=1000)
    await db_session.commit()

    assert row.status == "pending"
    assert row.attempts == 1
    assert row.last_error == "redis is down"
    assert row.next_attempt_at > before


async def test_sweep_published_deletes_only_aged_published_rows(db_session):
    repo = OutboxRepository(db_session)
    old = await repo.add(make_envelope("old"))
    recent = await repo.add(make_envelope("recent"))
    failed = await repo.add(make_envelope("failed"))
    pending = await repo.add(make_envelope("pending"))
    await repo.mark_published(old)
    await repo.mark_published(recent)
    await repo.mark_failed(failed, "boom")
    old.published_at = datetime.now(UTC) - timedelta(days=2)
    await db_session.commit()

    deleted = await repo.sweep_published(datetime.now(UTC) - timedelta(days=1))
    await db_session.commit()

    assert deleted == 1
    remaining = (await db_session.execute(select(OutboxEvent.correlation_id))).scalars()
    assert set(remaining) == {"recent", "failed", "pending"}
    assert pending.status == "pending"


async def test_two_concurrent_claims_never_return_the_same_row(db_session):
    """
    SKIP LOCKED is what lets several relay replicas share one table. This
    proves the claims do not overlap; it does NOT prove exactly-once delivery,
    which the architecture does not offer.
    """
    repo = OutboxRepository(db_session)
    for ref in ("r1", "r2", "r3", "r4"):
        await repo.add(make_envelope(ref))
    await db_session.commit()

    engine = create_async_engine(get_settings().DATABASE_URL)
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def claim_two() -> list[str]:
        async with maker() as session, session.begin():
            rows = await OutboxRepository(session).claim_batch(limit=2)
            # Hold the locks long enough for the other claimer to run.
            await asyncio.sleep(0.2)
            return [r.correlation_id for r in rows]

    first, second = await asyncio.gather(claim_two(), claim_two())
    await engine.dispose()

    assert set(first) & set(second) == set()
    assert len(first) + len(second) == 4
