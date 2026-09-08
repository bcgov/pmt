# tests/integration/test_outbox_model.py

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import select

from db.models import OutboxEvent

pytestmark = pytest.mark.integration


async def test_outbox_row_round_trips_with_pending_defaults(db_session):
    event_id = uuid4()
    db_session.add(
        OutboxEvent(
            event_id=event_id,
            event_type="OrderCreated",
            correlation_id="r1",
            source="api",
            payload='{"hello": "world"}',
        )
    )
    await db_session.commit()

    row = (
        await db_session.execute(
            select(OutboxEvent).where(OutboxEvent.event_id == event_id)
        )
    ).scalar_one()

    assert row.status == "pending"
    assert row.attempts == 0
    assert row.last_error is None
    assert row.published_at is None
    assert row.failed_at is None
    assert row.payload == '{"hello": "world"}'
    assert row.next_attempt_at <= datetime.now(UTC)


async def test_payload_is_stored_byte_for_byte(db_session):
    """
    TEXT, not JSONB: the relay publishes exactly what the writer serialized.
    JSONB would reorder keys and strip whitespace.
    """
    exact = '{"b": 1, "a": 2,   "c": [1.10, 2.0]}'
    db_session.add(
        OutboxEvent(
            event_id=uuid4(),
            event_type="OrderCreated",
            correlation_id="r2",
            source="api",
            payload=exact,
        )
    )
    await db_session.commit()

    row = (
        await db_session.execute(
            select(OutboxEvent).where(OutboxEvent.correlation_id == "r2")
        )
    ).scalar_one()
    assert row.payload == exact
