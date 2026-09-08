# Transactional Outbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write the order row and its `OrderCreated` event in one transaction, and publish the event from a background relay, so a committed order is always eventually announced.

**Architecture:** `OrderService.create_order` writes an `orders` row and an `outbox` row in a single transaction and never touches Redis. An `OutboxRelay` background task in the API process claims unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes each with `XADD`, and marks them published in the transaction that claimed them. A local `asyncio.Event` nudge cuts happy-path latency; the poll interval is the actual guarantee.

**Tech Stack:** Python 3.14, FastAPI, SQLAlchemy 2.0 async, asyncpg, Alembic, `redis.asyncio`, Pydantic v2, pytest / pytest-asyncio, testcontainers.

**Spec:** `docs/superpowers/specs/2026-09-07-transactional-outbox-design.md`

## Global Constraints

- Line length 88 (Black). Ruff selects `E, F, W, B, I`; `E501` ignored.
- Import order: standard library, third-party, local.
- Repositories flush, never commit. Transaction boundaries belong to the caller.
- Constructors must not open sockets or database connections. `RedisConsumer.__init__` is the reference.
- Schema changes go through Alembic only. Every revision implements `downgrade()`.
- The relay never parses `payload`. It is an opaque `str` moved from Postgres to Redis.
- Delivery is at-least-once by design. Do not add deduplication.
- Every new async test function needs no marker for unit tests; integration test modules set `pytestmark = pytest.mark.integration`.
- Run `make lint` before every commit.

---

### Task 1: `OutboxEvent` model and migration

**Files:**
- Modify: `db/models.py` (append after `Order`)
- Create: `db/migrations/versions/0003_create_outbox.py`
- Modify: `tests/conftest.py:110-114` (the `db_session` TRUNCATE)
- Test: `tests/integration/test_outbox_model.py`

**Interfaces:**
- Consumes: `Base` from `db/postgres/session.py`, `_utcnow` from `db/models.py`.
- Produces: `db.models.OutboxEvent` with columns `id, event_id, event_type, correlation_id, source, payload, status, attempts, last_error, next_attempt_at, created_at, published_at, failed_at`. Status values are the string literals `"pending"`, `"published"`, `"failed"`.

- [ ] **Step 1: Write the failing test**

Create `tests/integration/test_outbox_model.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/integration/test_outbox_model.py -v`
Expected: FAIL with `ImportError: cannot import name 'OutboxEvent' from 'db.models'`

- [ ] **Step 3: Add the model**

Append to `db/models.py`. Extend the existing `sqlalchemy` import line to
`from sqlalchemy import BigInteger, DateTime, Index, Integer, String, Text, Uuid, text`
and add `from uuid import UUID` to the standard-library imports.

```python
class OutboxEvent(Base):
    """
    One event waiting to be published, written in the same transaction as the
    domain row that produced it.

    `payload` is the exact serialized EventEnvelope. It is TEXT rather than
    JSONB deliberately: JSONB stores a parsed form that reorders keys, strips
    whitespace and normalizes numbers, so it cannot give back the bytes the
    writer produced. The relay treats this column as opaque and never parses
    it.
    """

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # The envelope's own id, lifted out so it is queryable and unique.
    event_id: Mapped[UUID] = mapped_column(Uuid, unique=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)

    payload: Mapped[str] = mapped_column(Text, nullable=False)

    # pending | published | failed
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")

    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        # Serves the relay's claim query.
        Index(
            "ix_outbox_pending",
            "next_attempt_at",
            "id",
            postgresql_where=text("status = 'pending'"),
        ),
        # Serves the retention sweep.
        Index(
            "ix_outbox_published",
            "published_at",
            postgresql_where=text("status = 'published'"),
        ),
    )

    def __repr__(self) -> str:
        return f"<OutboxEvent(id={self.id}, type={self.event_type}, status={self.status})>"
```

- [ ] **Step 4: Write the migration**

Create `db/migrations/versions/0003_create_outbox.py`:

```python
"""create outbox

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("correlation_id", sa.String(length=255), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id"),
    )
    op.create_index(
        "ix_outbox_pending",
        "outbox",
        ["next_attempt_at", "id"],
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_outbox_published",
        "outbox",
        ["published_at"],
        postgresql_where=sa.text("status = 'published'"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_published", table_name="outbox")
    op.drop_index("ix_outbox_pending", table_name="outbox")
    op.drop_table("outbox")
```

- [ ] **Step 5: Truncate the new table between tests**

In `tests/conftest.py`, the `db_session` fixture's cleanup currently reads:

```python
        await conn.execute(text("TRUNCATE TABLE orders RESTART IDENTITY CASCADE"))
```

Replace with:

```python
        await conn.execute(
            text("TRUNCATE TABLE orders, outbox RESTART IDENTITY CASCADE")
        )
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `poetry run pytest tests/integration/test_outbox_model.py tests/integration/test_migrations.py -v`
Expected: PASS. `test_migrations.py` exercises `upgrade head` -> `downgrade base` -> `upgrade head` and now covers revision 0003.

- [ ] **Step 7: Lint and commit**

```bash
make lint
git add db/models.py db/migrations/versions/0003_create_outbox.py tests/conftest.py tests/integration/test_outbox_model.py
git commit -m "feat: add outbox table for transactional event publishing"
```

---

### Task 2: `OutboxRepository`

**Files:**
- Create: `db/repositories/outbox_repository.py`
- Test: `tests/integration/test_outbox_repository.py`

**Interfaces:**
- Consumes: `db.models.OutboxEvent` (Task 1), `messaging.models.EventEnvelope`.
- Produces: `db.repositories.outbox_repository.OutboxRepository(session)` with
  `async add(envelope: EventEnvelope) -> OutboxEvent`,
  `async claim_batch(limit: int) -> list[OutboxEvent]`,
  `async mark_published(row: OutboxEvent) -> None`,
  `async mark_failed(row: OutboxEvent, error: str) -> None`,
  `async mark_retry(row: OutboxEvent, error: str, backoff_ms: int) -> None`,
  `async sweep_published(older_than: datetime) -> int`.

- [ ] **Step 1: Write the failing tests**

Create `tests/integration/test_outbox_repository.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/integration/test_outbox_repository.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'db.repositories.outbox_repository'`

- [ ] **Step 3: Write the repository**

Create `db/repositories/outbox_repository.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/integration/test_outbox_repository.py -v`
Expected: PASS (11 tests)

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add db/repositories/outbox_repository.py tests/integration/test_outbox_repository.py
git commit -m "feat: add OutboxRepository with SKIP LOCKED claim and retention sweep"
```

---

### Task 3: `RedisProducer.publish_raw`

**Files:**
- Modify: `messaging/producer/redis_producer.py:23-49`
- Test: `tests/integration/test_producer.py` (append)

**Interfaces:**
- Produces: `RedisProducer.publish_raw(payload: str) -> str`. `publish(envelope)` keeps its existing signature and return type and now delegates, so `XADD` has one call site.

- [ ] **Step 1: Write the failing test**

Append to `tests/integration/test_producer.py`:

```python
async def test_publish_raw_writes_the_string_through_untouched(
    app_settings, redis_client
):
    """
    The outbox relay publishes stored bytes it never parsed. Whatever string
    goes in must come out on the stream identically.
    """
    from messaging.producer.redis_producer import RedisProducer

    producer = RedisProducer()
    exact = '{"b": 1, "a": 2,   "c": [1.10, 2.0]}'

    message_id = await producer.publish_raw(exact)

    entries = await redis_client.xrange(app_settings.STREAM_NAME)
    assert len(entries) == 1
    assert entries[0][0] == message_id
    assert entries[0][1]["event"] == exact
    await producer.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/integration/test_producer.py::test_publish_raw_writes_the_string_through_untouched -v`
Expected: FAIL with `AttributeError: 'RedisProducer' object has no attribute 'publish_raw'`

- [ ] **Step 3: Add `publish_raw` and make `publish` delegate**

In `messaging/producer/redis_producer.py`, replace the body of `publish` with a
delegating wrapper and add `publish_raw` above it:

```python
    async def publish_raw(self, payload: str) -> str:
        """
        Publish an already-serialized envelope.

        The outbox relay uses this: it moves a stored string to Redis without
        ever parsing it, so the bytes the writer produced are the bytes that
        reach the stream. Message shape is {"event": "<json>"} — the consumer
        reads the same key.
        """
        try:
            message_id = await self.redis.xadd(
                name=self.stream_name,
                fields={"event": payload},
            )
            logger.info(
                "Event published",
                stream=self.stream_name,
                message_id=message_id,
            )
            return message_id
        except Exception as e:
            logger.error(
                "Failed to publish event",
                stream=self.stream_name,
                error=str(e),
                exc_info=True,
            )
            raise

    async def publish(self, envelope: EventEnvelope) -> str:
        """Serialize an envelope and publish it. One XADD call site: publish_raw."""
        return await self.publish_raw(envelope.model_dump_json())
```

- [ ] **Step 4: Run the producer tests**

Run: `poetry run pytest tests/integration/test_producer.py -v`
Expected: PASS — the pre-existing `publish` tests still pass through the new wrapper.

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add messaging/producer/redis_producer.py tests/integration/test_producer.py
git commit -m "feat: add RedisProducer.publish_raw for pre-serialized payloads"
```

---

### Task 4: Relay settings and error classification

**Files:**
- Modify: `config/settings.py` (new block after "Consumer reliability")
- Modify: `.env.example`
- Create: `messaging/outbox/__init__.py`
- Create: `messaging/outbox/backoff.py`
- Test: `tests/unit/test_outbox_backoff.py`

**Interfaces:**
- Produces: settings `RELAY_ENABLED: bool`, `OUTBOX_POLL_INTERVAL_MS: int`, `OUTBOX_BATCH_SIZE: int`, `OUTBOX_RETRY_BACKOFF_MS: int`, `OUTBOX_MAX_BACKOFF_MS: int`, `OUTBOX_RETENTION_HOURS: int`, `OUTBOX_SWEEP_INTERVAL_S: int`. Module `messaging.outbox.backoff` with `backoff_ms(attempts: int, base_ms: int, cap_ms: int) -> int` and `is_retryable(exc: BaseException) -> bool`.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_outbox_backoff.py`:

```python
from redis.exceptions import (
    BusyLoadingError,
    ConnectionError as RedisConnectionError,
    ReadOnlyError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)

from messaging.outbox.backoff import backoff_ms, is_retryable


def test_backoff_doubles_with_each_attempt():
    assert backoff_ms(0, base_ms=500, cap_ms=30_000) == 500
    assert backoff_ms(1, base_ms=500, cap_ms=30_000) == 1000
    assert backoff_ms(2, base_ms=500, cap_ms=30_000) == 2000
    assert backoff_ms(3, base_ms=500, cap_ms=30_000) == 4000


def test_backoff_is_capped():
    assert backoff_ms(20, base_ms=500, cap_ms=30_000) == 30_000


def test_transport_failures_are_retryable():
    for exc in (
        RedisConnectionError("down"),
        RedisTimeoutError("slow"),
        BusyLoadingError("loading"),
        ReadOnlyError("replica"),
    ):
        assert is_retryable(exc) is True


def test_response_errors_are_permanent():
    """
    WRONGTYPE or an oversized payload will never succeed on retry. Treating
    them as retryable would head-of-line block every row behind them.
    """
    assert is_retryable(ResponseError("WRONGTYPE")) is False


def test_unexpected_exceptions_are_permanent():
    assert is_retryable(ValueError("nonsense")) is False


def test_readonly_error_is_retryable_despite_subclassing_response_error():
    """
    redis-py's ReadOnlyError and BusyLoadingError both inherit ResponseError,
    so the retryable check must come first or a failover would dead-letter
    every row in flight.
    """
    assert issubclass(ReadOnlyError, ResponseError)
    assert is_retryable(ReadOnlyError("replica")) is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/unit/test_outbox_backoff.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'messaging.outbox'`

- [ ] **Step 3: Write the module**

Create `messaging/outbox/__init__.py`:

```python
# messaging/outbox/__init__.py
```

Create `messaging/outbox/backoff.py`:

```python
from redis.exceptions import (
    BusyLoadingError,
    ConnectionError as RedisConnectionError,
    ReadOnlyError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)

# Redis is unreachable or busy: the event is fine and must eventually go out.
# ReadOnlyError and BusyLoadingError both subclass ResponseError, so this
# tuple has to be checked before any ResponseError handling.
RETRYABLE = (
    RedisConnectionError,
    RedisTimeoutError,
    BusyLoadingError,
    ReadOnlyError,
)


def is_retryable(exc: BaseException) -> bool:
    """
    True when publishing might succeed later.

    Everything else — a ResponseError like WRONGTYPE, a payload over
    proto-max-bulk-len, an unexpected bug — will fail identically forever, so
    the relay marks it failed and moves on rather than blocking the rows
    behind it.
    """
    return isinstance(exc, RETRYABLE)


def backoff_ms(attempts: int, base_ms: int, cap_ms: int) -> int:
    """
    Exponential backoff for transport failures. `attempts` is the count before
    this failure is recorded, so the first retry waits `base_ms`.
    """
    return min(base_ms * (2**attempts), cap_ms)
```

- [ ] **Step 4: Add the settings**

In `config/settings.py`, after the "Consumer reliability" block:

```python
    # -------------------------
    # Outbox relay
    # -------------------------
    RELAY_ENABLED: bool = True
    OUTBOX_POLL_INTERVAL_MS: int = 200
    # Also caps duplicate amplification: a crash mid-batch republishes at most
    # this many rows.
    OUTBOX_BATCH_SIZE: int = 20
    OUTBOX_RETRY_BACKOFF_MS: int = 500
    OUTBOX_MAX_BACKOFF_MS: int = 30_000
    OUTBOX_RETENTION_HOURS: int = 24
    OUTBOX_SWEEP_INTERVAL_S: int = 300
```

In `.env.example`, after the "Consumer reliability" block:

```
# Outbox relay
RELAY_ENABLED=true
OUTBOX_POLL_INTERVAL_MS=200
OUTBOX_BATCH_SIZE=20
OUTBOX_RETRY_BACKOFF_MS=500
OUTBOX_MAX_BACKOFF_MS=30000
OUTBOX_RETENTION_HOURS=24
OUTBOX_SWEEP_INTERVAL_S=300
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `poetry run pytest tests/unit/test_outbox_backoff.py -v`
Expected: PASS (6 tests)

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add config/settings.py .env.example messaging/outbox/__init__.py messaging/outbox/backoff.py tests/unit/test_outbox_backoff.py
git commit -m "feat: add outbox relay settings and Redis error classification"
```

---

### Task 5: `OutboxRelay.drain_once`

**Files:**
- Create: `messaging/outbox/relay.py`
- Test: `tests/unit/test_outbox_relay.py`

**Interfaces:**
- Consumes: `OutboxRepository` (Task 2), `RedisProducer.publish_raw` (Task 3), `backoff_ms` / `is_retryable` (Task 4).
- Produces: `messaging.outbox.relay.OutboxRelay(producer=None, session_maker=None)` with `async drain_once() -> int` returning the number of rows claimed. Both constructor arguments are injection seams for tests; production passes neither.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_outbox_relay.py`:

```python
from datetime import UTC, datetime

from redis.exceptions import ConnectionError as RedisConnectionError, ResponseError

from messaging.outbox.relay import OutboxRelay


class FakeRow:
    def __init__(self, id: int, payload: str = '{"a": 1}'):
        self.id = id
        self.payload = payload
        self.status = "pending"
        self.attempts = 0
        self.last_error = None
        self.published_at = None
        self.failed_at = None
        self.next_attempt_at = datetime.now(UTC)


class FakeRepo:
    """Mirrors OutboxRepository's contract without a database."""

    def __init__(self, rows):
        self.rows = rows

    async def claim_batch(self, limit):
        return self.rows[:limit]

    async def mark_published(self, row):
        row.status = "published"
        row.published_at = datetime.now(UTC)

    async def mark_failed(self, row, error):
        row.status = "failed"
        row.failed_at = datetime.now(UTC)
        row.last_error = error

    async def mark_retry(self, row, error, backoff_ms):
        row.attempts += 1
        row.last_error = error
        row.backoff_ms = backoff_ms

    async def sweep_published(self, older_than):
        # The loop sweeps on its first iteration; Task 6's tests reuse this.
        return 0


class FakeProducer:
    def __init__(self, raise_on=None, exc=None):
        self.published = []
        self.raise_on = raise_on  # row id that fails
        self.exc = exc

    async def publish_raw(self, payload):
        if self.raise_on is not None and len(self.published) == self.raise_on:
            raise self.exc
        self.published.append(payload)
        return "1-0"


class FakeSession:
    def __init__(self):
        self.committed = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def begin(self):
        return _FakeBegin(self)


class _FakeBegin:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, *args):
        if exc_type is None:
            self.session.committed += 1
        return False


def make_relay(rows, producer):
    session = FakeSession()
    relay = OutboxRelay(producer=producer, session_maker=lambda: session)
    relay._repo_factory = lambda s: FakeRepo(rows)
    relay.batch_size = 10
    return relay, session


async def test_drain_publishes_every_row_and_marks_it_published():
    rows = [FakeRow(1), FakeRow(2), FakeRow(3)]
    producer = FakeProducer()
    relay, session = make_relay(rows, producer)

    claimed = await relay.drain_once()

    assert claimed == 3
    assert len(producer.published) == 3
    assert [r.status for r in rows] == ["published"] * 3
    assert session.committed == 1


async def test_transport_failure_keeps_the_row_pending_and_stops_the_batch():
    """
    If Redis is unreachable for one row it is unreachable for all of them.
    Breaking preserves publish order instead of burning attempts on every row.
    """
    rows = [FakeRow(1), FakeRow(2), FakeRow(3)]
    producer = FakeProducer(raise_on=1, exc=RedisConnectionError("down"))
    relay, _ = make_relay(rows, producer)

    await relay.drain_once()

    assert rows[0].status == "published"
    assert rows[1].status == "pending"
    assert rows[1].attempts == 1
    assert rows[1].last_error == "down"
    assert rows[2].status == "pending"
    assert rows[2].attempts == 0  # never attempted


async def test_permanent_failure_marks_the_row_and_continues_the_batch():
    """
    One unpublishable row must not block the rows behind it forever.
    """
    rows = [FakeRow(1), FakeRow(2), FakeRow(3)]
    producer = FakeProducer(raise_on=0, exc=ResponseError("WRONGTYPE"))
    relay, _ = make_relay(rows, producer)

    await relay.drain_once()

    assert rows[0].status == "failed"
    assert rows[0].last_error == "WRONGTYPE"
    assert rows[1].status == "published"
    assert rows[2].status == "published"


async def test_backoff_grows_with_the_attempt_count():
    row = FakeRow(1)
    row.attempts = 2
    producer = FakeProducer(raise_on=0, exc=RedisConnectionError("down"))
    relay, _ = make_relay([row], producer)
    relay.retry_backoff_ms = 500
    relay.max_backoff_ms = 30_000

    await relay.drain_once()

    assert row.backoff_ms == 2000


async def test_empty_payload_is_a_permanent_integrity_failure():
    """
    Unreachable in practice — the writer serializes a validated envelope in
    the same transaction as the order row. It exists so corruption surfaces as
    a failed row instead of a crashed relay.
    """
    row = FakeRow(1, payload="")
    producer = FakeProducer()
    relay, _ = make_relay([row], producer)

    await relay.drain_once()

    assert row.status == "failed"
    assert producer.published == []


async def test_an_empty_claim_publishes_nothing():
    producer = FakeProducer()
    relay, session = make_relay([], producer)

    assert await relay.drain_once() == 0
    assert producer.published == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/unit/test_outbox_relay.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'messaging.outbox.relay'`

- [ ] **Step 3: Write the relay's drain path**

Create `messaging/outbox/relay.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/unit/test_outbox_relay.py -v`
Expected: PASS (6 tests)

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add messaging/outbox/relay.py tests/unit/test_outbox_relay.py
git commit -m "feat: add OutboxRelay batch drain with retry and dead-letter paths"
```

---

### Task 6: The relay loop, nudge, sweep and singleton

**Files:**
- Modify: `messaging/outbox/relay.py`
- Modify: `messaging/outbox/__init__.py`
- Test: `tests/unit/test_outbox_relay.py` (append)

**Interfaces:**
- Produces: `OutboxRelay.start()`, `OutboxRelay.stop()`, `OutboxRelay.notify()`, `OutboxRelay.close()`; module functions `get_relay() -> OutboxRelay` and `async close_relay() -> None`, exported from `messaging.outbox`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_outbox_relay.py`:

```python
import asyncio

import pytest

from messaging.outbox.relay import close_relay, get_relay


async def test_notify_wakes_the_loop_before_the_poll_interval_elapses():
    """
    The nudge is a latency optimization only: it reaches the relay in this
    process, so the poll interval is still the actual guarantee.
    """
    relay = OutboxRelay(producer=FakeProducer(), session_maker=lambda: FakeSession())
    relay._repo_factory = lambda s: FakeRepo([])
    relay.poll_interval_ms = 60_000  # long enough that only notify() can win

    task = asyncio.create_task(relay.start())
    await asyncio.sleep(0.05)
    drains_before = relay.drain_count

    relay.notify()
    await asyncio.sleep(0.05)

    assert relay.drain_count > drains_before

    await relay.stop()
    await asyncio.wait_for(task, timeout=5)


async def test_stop_ends_the_loop():
    relay = OutboxRelay(producer=FakeProducer(), session_maker=lambda: FakeSession())
    relay._repo_factory = lambda s: FakeRepo([])
    relay.poll_interval_ms = 10

    task = asyncio.create_task(relay.start())
    await asyncio.sleep(0.05)
    await relay.stop()

    await asyncio.wait_for(task, timeout=5)
    assert relay.running is False


async def test_a_drain_error_does_not_kill_the_loop():
    """
    A database blip must not silently end publishing for the process's life.
    """
    relay = OutboxRelay(producer=FakeProducer(), session_maker=lambda: FakeSession())
    calls = {"n": 0}

    async def exploding_drain():
        calls["n"] += 1
        raise RuntimeError("database is gone")

    relay.drain_once = exploding_drain
    relay.poll_interval_ms = 10

    task = asyncio.create_task(relay.start())
    await asyncio.sleep(0.1)
    await relay.stop()
    await asyncio.wait_for(task, timeout=5)

    assert calls["n"] > 1


async def test_get_relay_returns_one_instance_per_process():
    await close_relay()
    assert get_relay() is get_relay()
    await close_relay()


async def test_notify_before_start_is_safe():
    """
    create_order calls notify() unconditionally, including when RELAY_ENABLED
    is false and nothing ever started a loop.
    """
    relay = OutboxRelay(producer=FakeProducer(), session_maker=lambda: FakeSession())
    relay.notify()  # must not raise
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/unit/test_outbox_relay.py -v`
Expected: FAIL with `ImportError: cannot import name 'close_relay' from 'messaging.outbox.relay'`

- [ ] **Step 3: Add the loop, the sweep and the singleton**

In `messaging/outbox/relay.py`, add these imports to the top:

```python
import asyncio
import time
from datetime import UTC, datetime, timedelta
```

Extend `__init__` with the loop's state, after the backoff settings:

```python
        self.poll_interval_ms = settings.OUTBOX_POLL_INTERVAL_MS
        self.retention = timedelta(hours=settings.OUTBOX_RETENTION_HOURS)
        self.sweep_interval_s = settings.OUTBOX_SWEEP_INTERVAL_S

        self.running = False
        self.drain_count = 0
        # Built here, not in start(): create_order calls notify() and must not
        # care whether a loop is running in this process.
        self._wake = asyncio.Event()
        self._last_sweep = 0.0
```

Increment `self.drain_count += 1` as the first statement of `drain_once`, then
append the loop, sweep, control methods and singleton to the module:

```python
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
```

Note: `_maybe_sweep` runs on the first loop iteration because `_last_sweep`
starts at `0.0`. That is intentional — a restart clears rows that aged out
while the process was down.

Replace `messaging/outbox/__init__.py` with:

```python
# messaging/outbox/__init__.py

from .relay import OutboxRelay, close_relay, get_relay

__all__ = ["OutboxRelay", "get_relay", "close_relay"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/unit/test_outbox_relay.py -v`
Expected: PASS (11 tests)

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add messaging/outbox/relay.py messaging/outbox/__init__.py tests/unit/test_outbox_relay.py
git commit -m "feat: add outbox relay loop with nudge, retention sweep and singleton"
```

---

### Task 7: `OrderService` writes the outbox instead of publishing

**Files:**
- Modify: `core/services/order_service.py`
- Modify: `api/routes/orders.py:16-45`
- Test: `tests/unit/test_order_service.py` (rewrite)

**Interfaces:**
- Consumes: `OutboxRepository.add` (Task 2), `get_relay` (Task 6).
- Produces: `OrderService(session)` — the `producer` parameter is gone — and `async create_order(order_ref, item, quantity) -> Order`, no longer a tuple. `service.outbox` is the `OutboxRepository` instance, replaceable in tests the way `service.repo` already is.

- [ ] **Step 1: Rewrite the failing tests**

Replace `tests/unit/test_order_service.py` entirely:

```python
import pytest
from sqlalchemy.exc import IntegrityError

from core.services.order_service import DuplicateOrderError, OrderService
from db.models import Order


class FakeRepo:
    def __init__(self, existing=None):
        self.existing = existing
        self.created = None

    async def get_by_ref(self, order_ref):
        return self.existing

    async def create(self, order_ref, item, quantity):
        self.created = Order(
            order_ref=order_ref, item=item, quantity=quantity, status="pending"
        )
        return self.created


class FakeOutbox:
    def __init__(self):
        self.added = []

    async def add(self, envelope):
        self.added.append(envelope)
        return envelope


class FakeSession:
    def __init__(self, fail_commit=False):
        self.commits = 0
        self.rolled_back = False
        self.fail_commit = fail_commit

    @property
    def committed(self) -> bool:
        return self.commits > 0

    async def commit(self):
        if self.fail_commit:
            raise IntegrityError("INSERT", {}, Exception("duplicate key"))
        self.commits += 1

    async def rollback(self):
        self.rolled_back = True


class FakeRelay:
    def __init__(self):
        self.notified = 0

    def notify(self):
        self.notified += 1


@pytest.fixture
def relay(monkeypatch):
    """Replace the process-wide relay so notify() is observable."""
    fake = FakeRelay()
    monkeypatch.setattr("core.services.order_service.get_relay", lambda: fake)
    return fake


@pytest.fixture(autouse=True)
def no_redis_on_the_request_path(monkeypatch):
    """
    create_order must not touch Redis at all. OrderService no longer takes a
    producer, so guard the module-level accessor instead: any call fails.
    """

    def explode():
        raise AssertionError("create_order must not touch Redis")

    monkeypatch.setattr("messaging.producer.redis_producer.get_producer", explode)


def make_service(session=None, repo=None, outbox=None):
    service = OrderService(session or FakeSession())
    service.repo = repo or FakeRepo()
    service.outbox = outbox or FakeOutbox()
    return service


async def test_create_order_writes_both_rows_and_commits_exactly_once(relay):
    session, outbox = FakeSession(), FakeOutbox()
    service = make_service(session=session, outbox=outbox)

    order = await service.create_order("r1", "widget", 2)

    assert session.commits == 1
    assert order.status == "pending"
    assert len(outbox.added) == 1
    envelope = outbox.added[0]
    assert envelope.event_type == "OrderCreated"
    assert envelope.payload.order_ref == "r1"
    assert envelope.correlation_id == "r1"
    assert envelope.source == "api"


async def test_create_order_nudges_the_relay_after_committing(relay):
    service = make_service()

    await service.create_order("r1", "widget", 2)

    assert relay.notified == 1


async def test_duplicate_order_ref_is_rejected_before_any_write(relay):
    existing = Order(order_ref="r1", item="widget", quantity=1, status="pending")
    session, outbox = FakeSession(), FakeOutbox()
    service = make_service(
        session=session, repo=FakeRepo(existing=existing), outbox=outbox
    )

    with pytest.raises(DuplicateOrderError):
        await service.create_order("r1", "widget", 2)

    assert session.commits == 0
    assert outbox.added == []
    assert relay.notified == 0


async def test_concurrent_duplicate_is_caught_by_the_unique_constraint(relay):
    """
    The get_by_ref check and the insert are not atomic: two concurrent
    requests can both pass the check, so the unique constraint is the real
    guard. Its IntegrityError must map onto the same DuplicateOrderError, and
    the outbox row rolls back with the order row.
    """
    session = FakeSession(fail_commit=True)
    service = make_service(session=session)

    with pytest.raises(DuplicateOrderError):
        await service.create_order("r1", "widget", 2)

    assert session.rolled_back is True
    assert relay.notified == 0


async def test_get_order_delegates_to_the_repository(relay):
    existing = Order(order_ref="r1", item="widget", quantity=1, status="pending")
    service = make_service(repo=FakeRepo(existing=existing))

    assert await service.get_order("r1") is existing
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/unit/test_order_service.py -v`
Expected: FAIL — `AttributeError` on `core.services.order_service.get_relay`, and `create_order` still returning a tuple.

- [ ] **Step 3: Rewrite the service**

Replace `core/services/order_service.py` with:

```python
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging import get_logger
from db.models import Order
from db.repositories.order_repository import OrderRepository
from db.repositories.outbox_repository import OutboxRepository
from messaging.models import EventEnvelope, OrderCreatedEvent
from messaging.outbox.relay import get_relay

logger = get_logger(__name__)


class DuplicateOrderError(Exception):
    """Raised when order_ref is already taken."""


class OrderService:
    """
    Business logic for orders.

    Sequencing note: the order row and its event are written in ONE
    transaction — the event goes to the `outbox` table, not to Redis. This
    method never publishes. The relay does that, after the commit, which is
    what makes "the row exists but the event does not" impossible.
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self.repo = OrderRepository(session)
        self.outbox = OutboxRepository(session)

    async def create_order(self, order_ref: str, item: str, quantity: int) -> Order:
        if await self.repo.get_by_ref(order_ref) is not None:
            raise DuplicateOrderError(f"order_ref already exists: {order_ref}")

        order = await self.repo.create(
            order_ref=order_ref, item=item, quantity=quantity
        )

        envelope = EventEnvelope.create(
            event_type="OrderCreated",
            payload=OrderCreatedEvent(
                order_ref=order_ref, item=item, quantity=quantity
            ),
            correlation_id=order_ref,
            source="api",
        )
        await self.outbox.add(envelope)

        try:
            await self.session.commit()
        except IntegrityError as e:
            # Two concurrent requests can both pass the check above; the
            # unique constraint is the real guard, this just maps its
            # failure onto the same 409 the explicit check raises. The outbox
            # row rolls back with the order row — that is the point.
            await self.session.rollback()
            raise DuplicateOrderError(f"order_ref already exists: {order_ref}") from e

        logger.info("Order created", order_ref=order_ref, status=order.status)

        # Latency only: wakes a relay in this process so the event does not
        # wait out the poll interval. Harmless when RELAY_ENABLED is false.
        get_relay().notify()

        return order

    async def get_order(self, order_ref: str) -> Order | None:
        return await self.repo.get_by_ref(order_ref)
```

- [ ] **Step 4: Update the route**

In `api/routes/orders.py`, replace the `create_order` handler with:

```python
@router.post("", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    request: CreateOrderRequest, db: AsyncSession = Depends(get_db)
) -> OrderResponse:
    """
    Persist an order as `pending` and queue OrderCreated in the outbox.

    Both rows commit together, so a 201 means the event will be published.
    The status is `pending` until the consumer confirms it.
    """
    service = OrderService(db)
    try:
        order = await service.create_order(
            order_ref=request.order_ref,
            item=request.item,
            quantity=request.quantity,
        )
    except DuplicateOrderError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e

    return OrderResponse.model_validate(order)
```

The `logger` import stays — the module's `get_order` handler and module-level
logger are unchanged.

- [ ] **Step 5: Run the unit suite**

Run: `poetry run pytest -m "not integration" -v`
Expected: PASS. `tests/unit/test_main.py` must still pass; if it asserts on the
lifespan's tasks it is updated in Task 8, not here.

- [ ] **Step 6: Lint and commit**

```bash
make lint
git add core/services/order_service.py api/routes/orders.py tests/unit/test_order_service.py
git commit -m "feat: write OrderCreated to the outbox in the order's transaction"
```

---

### Task 8: Start the relay in the application lifespan

**Files:**
- Modify: `main.py:20-70`
- Test: `tests/unit/test_main.py` (append)

**Interfaces:**
- Consumes: `get_relay`, `close_relay` (Task 6), `RELAY_ENABLED` (Task 4).
- Produces: module-level `relay` and `relay_task` in `main.py`, alongside the existing `consumer` / `consumer_task`.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_main.py`:

```python
async def test_lifespan_starts_and_stops_the_relay(monkeypatch):
    import main

    started, stopped = [], []

    class FakeRelay:
        async def start(self):
            started.append(True)
            await asyncio.Event().wait()  # runs until cancelled

        async def stop(self):
            stopped.append(True)

    class FakeConsumer:
        async def start(self):
            await asyncio.Event().wait()

        async def stop(self):
            pass

        async def close(self):
            pass

    monkeypatch.setattr(main, "RedisConsumer", lambda: FakeConsumer())
    monkeypatch.setattr(main, "get_relay", lambda: FakeRelay())
    monkeypatch.setattr(main, "close_producer", _noop)
    monkeypatch.setattr(main, "close_relay", _noop)
    monkeypatch.setattr(main, "close_db", _noop)

    async with main.lifespan(main.app):
        assert started == [True]

    assert stopped == [True]


async def test_relay_is_not_started_when_disabled(monkeypatch):
    """RELAY_ENABLED=false lets the relay run in another deployment instead."""
    import main
    from config.settings import get_settings

    started = []

    class FakeRelay:
        async def start(self):
            started.append(True)

        async def stop(self):
            pass

    class FakeConsumer:
        async def start(self):
            await asyncio.Event().wait()

        async def stop(self):
            pass

        async def close(self):
            pass

    settings = get_settings()
    monkeypatch.setattr(settings, "RELAY_ENABLED", False)
    monkeypatch.setattr(main, "RedisConsumer", lambda: FakeConsumer())
    monkeypatch.setattr(main, "get_relay", lambda: FakeRelay())
    monkeypatch.setattr(main, "close_producer", _noop)
    monkeypatch.setattr(main, "close_relay", _noop)
    monkeypatch.setattr(main, "close_db", _noop)

    async with main.lifespan(main.app):
        pass

    assert started == []
```

Add at the top of the file, next to the existing imports, whatever is missing:

```python
import asyncio


async def _noop(*args, **kwargs):
    return None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `poetry run pytest tests/unit/test_main.py -v`
Expected: FAIL with `AttributeError: module 'main' has no attribute 'get_relay'`

- [ ] **Step 3: Wire the relay into the lifespan**

In `main.py`, add to the imports:

```python
from config.settings import get_settings
from messaging.outbox import close_relay, get_relay
```

Replace the globals block:

```python
# Global consumer instance
consumer = None
consumer_task = None
relay = None
relay_task = None
```

Extend the existing `global consumer, consumer_task` statement at the top of
`lifespan` to:

```python
    global consumer, consumer_task, relay, relay_task
```

Then, after the consumer's `logger.info("Redis Stream consumer started")`:

```python
    if get_settings().RELAY_ENABLED:
        relay = get_relay()
        relay_task = asyncio.create_task(relay.start())
        relay_task.add_done_callback(
            lambda t: (
                logger.error("Relay task died", error=str(t.exception()))
                if not t.cancelled() and t.exception()
                else None
            )
        )
        logger.info("Outbox relay started")
    else:
        logger.info("Outbox relay disabled; rows will wait for another process")
```

After the consumer shutdown block and before `await close_producer()`:

```python
    if relay:
        await relay.stop()
    if relay_task:
        try:
            # Let the in-flight batch finish before giving up on it.
            await asyncio.wait_for(relay_task, timeout=10)
        except TimeoutError:
            logger.warning("Relay did not stop in time; cancelling")
            relay_task.cancel()
            await asyncio.gather(relay_task, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # Whatever killed the relay must not skip the cleanup below.
            logger.error("Relay task ended with error", error=str(e))
        logger.info("Outbox relay stopped")

    await close_relay()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest -m "not integration" -v`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add main.py tests/unit/test_main.py
git commit -m "feat: run the outbox relay in the application lifespan"
```

---

### Task 9: End-to-end integration

**Files:**
- Create: `tests/integration/test_outbox_relay_integration.py`
- Modify: `tests/integration/test_order_roundtrip.py`

**Interfaces:**
- Consumes: everything from Tasks 1-8.

- [ ] **Step 1: Write the failing tests**

Create `tests/integration/test_outbox_relay_integration.py`:

```python
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
    relays = [
        OutboxRelay(producer=p, session_maker=session_maker) for p in producers
    ]
    for relay in relays:
        relay.batch_size = 3

    await asyncio.gather(*(relay.drain_once() for relay in relays))
    for p in producers:
        await p.close()

    entries = await redis_client.xrange(app_settings.STREAM_NAME)
    refs = [json.loads(e[1]["event"])["correlation_id"] for e in entries]
    assert sorted(refs) == ["r1", "r2", "r3", "r4", "r5", "r6"]

    statuses = (
        await db_session.execute(select(OutboxEvent.status))
    ).scalars().all()
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
        await db_session.execute(select(OutboxEvent.correlation_id))
    ).scalars().all()
    assert sorted(remaining) == ["failed", "pending"]
    assert pending.status == "pending"
```

- [ ] **Step 2: Run tests to verify they fail or pass**

Run: `poetry run pytest tests/integration/test_outbox_relay_integration.py -v`
Expected: PASS — Tasks 1-8 already provide everything. If any fail, fix the
implementation, not the test.

- [ ] **Step 3: Update the round-trip test**

In `tests/integration/test_order_roundtrip.py`, the `running_consumer` fixture
now also needs a relay, or nothing will ever publish. Add above it:

```python
from messaging.outbox.relay import OutboxRelay


@pytest.fixture
async def running_relay(app_settings, migrated_db, redis_client):
    """A live relay for the duration of one test."""
    relay = OutboxRelay()
    task = asyncio.create_task(relay.start())
    yield relay
    await relay.stop()
    await asyncio.wait_for(task, timeout=10)
    await relay.close()
```

Change the test signature to take it, and update the docstring:

```python
async def test_order_goes_from_pending_to_confirmed(
    app_settings, running_relay, running_consumer, redis_client, db_session
):
    """
    POST /orders -> order row and outbox row commit together -> relay
    publishes -> consumer confirms -> GET shows confirmed. The whole template
    in one test.
    """
```

Everything below the docstring is unchanged.

- [ ] **Step 4: Run the whole suite**

Run: `make test-all`
Expected: PASS

- [ ] **Step 5: Lint and commit**

```bash
make lint
git add tests/integration/test_outbox_relay_integration.py tests/integration/test_order_roundtrip.py
git commit -m "test: cover the outbox relay end to end"
```

---

### Task 10: Documentation

**Files:**
- Modify: `README.md:139-151`
- Modify: `CLAUDE.md`

**Interfaces:** none.

- [ ] **Step 1: Replace the README's known-limitation section**

Delete `README.md` lines 139-151 — the whole "## Known limitation: no
transactional outbox" section — and put this in its place:

```markdown
## Outbox

`POST /orders` writes two rows in one transaction: the order, and its
`OrderCreated` event in the `outbox` table. It never talks to Redis. A `201`
therefore means the event will be published, not that it has been.

`OutboxRelay` (`messaging/outbox/relay.py`) runs as a background task beside
the consumer. Each pass claims a batch with
`SELECT ... WHERE status='pending' ORDER BY id FOR UPDATE SKIP LOCKED`,
publishes each row's stored payload with `XADD`, and marks it `published` — all
in the transaction that claimed it. `create_order` nudges the relay after
committing so the event does not wait out the poll interval; the poll is still
what guarantees delivery.

`payload` is `TEXT`, not `JSONB`, on purpose: the writer serializes the
validated envelope once and the relay publishes that exact string. JSONB would
reorder keys and strip whitespace, and the relay would have to re-encode what
it was given.

Failure handling splits by cause:

- **Redis unreachable** (`ConnectionError`, `TimeoutError`, `BusyLoadingError`,
  `ReadOnlyError`): the row stays `pending`, `attempts` grows, and
  `next_attempt_at` backs off exponentially up to `OUTBOX_MAX_BACKOFF_MS`. The
  batch stops there — if Redis is down for one row it is down for all of them,
  and stopping preserves publish order.
- **Permanent rejection** (`WRONGTYPE`, an oversized payload, a corrupt row):
  the row becomes `failed` with `failed_at` and `last_error`, and the batch
  keeps going so one bad row cannot block the rest.

A `failed` row is the dead letter. There is no relay DLQ stream: the row
already holds the payload, the error and the timestamps, and marking it must
not depend on the Redis write that just failed. Query them with
`SELECT * FROM outbox WHERE status = 'failed'`.

Settings: `RELAY_ENABLED`, `OUTBOX_POLL_INTERVAL_MS`, `OUTBOX_BATCH_SIZE`,
`OUTBOX_RETRY_BACKOFF_MS`, `OUTBOX_MAX_BACKOFF_MS`, `OUTBOX_RETENTION_HOURS`,
`OUTBOX_SWEEP_INTERVAL_S`. Published rows are swept once they age past the
retention window; `failed` rows are never swept.

### What this still does not give you

- Delivery is at-least-once. A crash between `XADD` and the marking commit
  republishes the row, so handlers must be idempotent — the order handler's
  conditional `UPDATE ... WHERE status='pending'` is the pattern.
- One relay transaction covers a whole batch, so that crash republishes every
  row already published in the batch, not just one. Lower `OUTBOX_BATCH_SIZE`
  to narrow the window, at the cost of more database transactions.
- The batch's `XADD` calls run inside the open transaction, so a slow Redis
  keeps a pooled connection checked out and delays `VACUUM` cleanup for as
  long as the batch runs.
- Publish order is not guaranteed globally across replicas: `SKIP LOCKED` lets
  a later row overtake an earlier one another relay holds. Per-aggregate
  ordering needs partitioning by `correlation_id`, which this template does
  not do.
- The relay shares the API's process and event loop. Moving it to its own
  container is a deployment change, not a code change — set `RELAY_ENABLED=false`
  on the API and true on one dedicated deployment.
```

- [ ] **Step 2: Update CLAUDE.md**

In the Architecture section's messaging bullet list, after the
`messaging/models/` entry, add:

```markdown
   - `messaging/outbox/relay.py`: `OutboxRelay` — claims `outbox` rows with
     `SKIP LOCKED` and publishes them; `messaging/outbox/backoff.py` classifies
     Redis errors as retryable or permanent.
```

In the Data Access Layer bullet, change the repositories line to:

```markdown
   - `db/repositories/`: query logic (`order_repository.py`,
     `outbox_repository.py`).
```

Replace the Core Layer's description of `order_service.py` with:

```markdown
2. **Core Layer** (`core/services/`): Application services that sequence data
   and messaging work. `order_service.py` writes the order row and its
   `OrderCreated` event to the `outbox` table in one transaction and never
   publishes; the relay does that afterwards.
```

Add a new section after "## Consumer":

```markdown
## Outbox

`core/services/order_service.py` writes the domain row and its event in one
transaction — the event goes to the `outbox` table, never straight to Redis.
`messaging/outbox/relay.py` (`OutboxRelay`) runs as a background task in the
lifespan and publishes those rows.

- `payload` is `TEXT` holding the exact serialized `EventEnvelope`. The relay
  never parses it. Do not change this column to `JSONB` — JSONB reorders keys
  and strips whitespace, so it cannot return the bytes the writer produced.
- One transaction per batch: claim with `FOR UPDATE SKIP LOCKED`, `XADD` each
  row, mark each `published`, commit. A crash mid-batch republishes every row
  already published in it; `OUTBOX_BATCH_SIZE` bounds that.
- Redis being unreachable is retryable — the row stays `pending` with an
  exponential backoff and the batch stops. Anything else is permanent: the row
  becomes `failed` and the batch continues. `messaging/outbox/backoff.py`
  draws that line, and the retryable check must precede any `ResponseError`
  handling because `ReadOnlyError` and `BusyLoadingError` subclass it.
- A `failed` row is the dead letter. The relay never publishes to
  `DLQ_STREAM_NAME`; SQL stays authoritative because Redis is what may have
  failed.
- Delivery is at-least-once by design. Handlers must be idempotent.
- New event types need no relay change — the relay is domain-agnostic.
```

- [ ] **Step 3: Verify the whole suite and the running stack**

```bash
make lint
make test-all
make up && make demo && make down
```

Expected: lint clean, all tests pass, `make demo` shows an order going from
`pending` to `confirmed`.

- [ ] **Step 4: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "docs: replace the outbox limitation section with the mechanism"
```

---

## Verification

After Task 10, the spec's claims should all hold:

- `grep -rn "get_producer\|publish(" core/ api/` returns nothing — the request
  path has no Redis call.
- `SELECT * FROM outbox` after `make demo` shows one `published` row.
- `make test` passes without Docker; `make test-all` passes with it.
- `README.md` no longer contains "Known limitation: no transactional outbox".
