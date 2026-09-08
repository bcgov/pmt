# Transactional Outbox

**Date:** 2026-09-07
**Status:** Approved, ready for implementation planning

## Problem

`OrderService.create_order` commits the order row and then publishes
`OrderCreated` with `XADD`. The two are separate operations. When the commit
succeeds and the publish fails, the row exists and the event does not: the
order sits at `pending` forever, because only the consumer moves it to
`confirmed` and the consumer never hears about it.

The template currently documents this as a known limitation and returns 201
with `status: "pending"`, which is truthful but leaves the service unable to
guarantee that a committed order is ever announced.

This design closes the gap. The `README.md` "Known limitation: no
transactional outbox" section is removed and replaced by a section describing
the mechanism below and the narrower limitations that remain.

## Approach

The order row and its event are written in one transaction. The event goes to
an `outbox` table. A relay running as a background task in the API process
reads unpublished rows and publishes them to Redis Streams, marking each row
published in the same transaction that claimed it.

`create_order` never touches Redis. There is exactly one `XADD` call site for
domain events in the service: the relay.

When `RELAY_ENABLED` is false, `create_order` still writes the outbox row and
still calls `notify()` — the call is a no-op against a relay nobody started,
and the rows wait for a process that does run one. The write path does not
branch on the setting.

### Decisions and their reasons

**In-process relay, not a separate container.** The template already runs one
background asyncio task (`RedisConsumer`, started in `main.py`'s lifespan).
The relay is the second, started the same way. This keeps the template a
single container and reuses a pattern a reader has already met. The cost is
that relay throughput shares the API's event loop, and that multiple API
replicas all run a relay — handled by `FOR UPDATE SKIP LOCKED`.

**Outbox only, with a local nudge.** `create_order` writes both rows and
commits; it does not publish. To avoid paying the full poll interval on the
happy path, it then sets an `asyncio.Event` the relay waits on. The nudge only
reaches the relay in the same process, so it is a latency optimization and
never a correctness mechanism — the poll is the guarantee.

**`payload` is `TEXT`, not `JSONB`.** The goal is that the writer serializes
the envelope exactly once and the relay publishes bytes it never interprets.
`JSONB` cannot provide that: PostgreSQL stores a decomposed binary form,
stripping insignificant whitespace, dropping duplicate keys, reordering object
keys, and normalizing numeric literals. What comes back is equivalent JSON,
not the same JSON, and it arrives as a `dict` that must be re-encoded before
`XADD`. `TEXT` stores the exact wire representation. `BYTEA` would work too but
buys nothing: the envelope is UTF-8 JSON and `RedisProducer` runs with
`decode_responses=True`, so `redis-py` wants `str`.

The columns worth searching on (`event_type`, `correlation_id`, `source`,
`event_id`) are lifted out as their own columns, so losing the ability to
query inside `payload` costs nothing in practice.

**No relay-side validation.** `EventEnvelope` is Pydantic-validated at
construction and serialized before the insert, inside the same transaction as
the order row. There is no path from a committed row to an unparseable payload
except database corruption. The relay therefore does not parse `payload` at
all — re-validating what was already validated would be ceremony, and parsing
is the only thing that could make the relay domain-aware.

**A failed row is the dead letter; there is no relay DLQ stream.** The
consumer needs a Redis DLQ because a Redis message has nowhere else to live. An
outbox row does: a row with `status='failed'` already carries the full payload,
`last_error`, `attempts`, and timestamps, and is queryable with `SELECT`.
Publishing a diagnostic copy to Redis would add a second record that can
silently fail to exist — and would make the SQL row's terminal state depend on
the very Redis write that is failing. SQL is authoritative.

**Delivery is at-least-once, deliberately.** If the process dies between a
successful `XADD` and the commit that marks the row published, the row is still
`pending` and will be published again. The consumer's conditional
`UPDATE ... WHERE status='pending'` already absorbs redelivery. This window is
the design's accepted seam, not a defect to engineer away.

## Data model

New model `OutboxEvent` in `db/models.py`, table `outbox`, created by
`db/migrations/versions/0003_create_outbox.py`. The revision implements
`downgrade()`; `tests/integration/test_migrations.py` exercises
`upgrade head` -> `downgrade base` -> `upgrade head`.

| column | type | notes |
| --- | --- | --- |
| `id` | `BigInteger`, PK, autoincrement | publish order |
| `event_id` | `UUID`, unique, not null | the envelope's own `event_id` |
| `event_type` | `String(100)`, not null | searchable |
| `correlation_id` | `String(255)`, not null | searchable |
| `source` | `String(50)`, not null | searchable |
| `payload` | `Text`, not null | exact serialized envelope; opaque to the relay |
| `status` | `String(20)`, not null, default `pending` | `pending` \| `published` \| `failed` |
| `attempts` | `Integer`, not null, default `0` | transport retries so far |
| `last_error` | `Text`, nullable | most recent failure |
| `next_attempt_at` | `DateTime(timezone=True)`, not null, default now | backoff gate |
| `created_at` | `DateTime(timezone=True)`, not null, default now | |
| `published_at` | `DateTime(timezone=True)`, nullable | terminal: published |
| `failed_at` | `DateTime(timezone=True)`, nullable | terminal: failed |

Indexes:

- `ix_outbox_pending` on `(next_attempt_at, id) WHERE status = 'pending'` —
  serves the claim query.
- `ix_outbox_published` on `(published_at) WHERE status = 'published'` —
  serves the retention sweep.
- unique constraint on `event_id`.

Both partial indexes are declared with SQLAlchemy's
`Index(..., postgresql_where=...)`.

## Components

### `db/repositories/outbox_repository.py` — `OutboxRepository`

Follows `OrderRepository`'s contract: flushes, never commits. Transaction
boundaries belong to the caller.

- `add(envelope: EventEnvelope) -> OutboxEvent` — serializes the envelope with
  `model_dump_json()` and inserts the row with the searchable columns lifted
  out of it.
- `claim_batch(limit: int) -> list[OutboxEvent]` — the claim query below.
- `mark_published(row) -> None`, `mark_failed(row, error: str) -> None`,
  `mark_retry(row, error: str, backoff_ms: int) -> None`.
- `sweep_published(older_than: datetime) -> int` — the retention delete.

Claim query:

```sql
SELECT * FROM outbox
WHERE status = 'pending' AND next_attempt_at <= now()
ORDER BY id
LIMIT :batch
FOR UPDATE SKIP LOCKED
```

### `messaging/producer/redis_producer.py` — `publish_raw`

`RedisProducer` gains `publish_raw(payload: str) -> str`, which does the
`XADD` with `fields={"event": payload}`. The existing
`publish(envelope)` becomes a thin wrapper that serializes and delegates, so
`XADD` keeps exactly one call site.

### `messaging/outbox/relay.py` — `OutboxRelay`

Module-level `get_relay()` / `close_relay()` singletons, mirroring
`get_producer()` / `close_producer()`. Constructing a relay must not open a
socket or a database connection, matching `RedisConsumer.__init__`'s rule.

- `start()` — the loop, run as an asyncio task.
- `stop()` — sets the stop flag and wakes the loop.
- `notify()` — sets the wake `asyncio.Event`; safe to call from request
  handlers.

Loop:

```
while running:
    n = await drain_once()
    maybe_sweep()
    if n == 0:
        wait for the wake event, with an OUTBOX_POLL_INTERVAL_MS timeout
```

`drain_once()` opens one session from `get_session_maker()`, claims a batch,
and for each row in `id` order:

```
XADD payload
  |-- ok                -> status='published', published_at=now()
  |-- transport error   -> attempts++, last_error, next_attempt_at=now()+backoff
  |                        row stays 'pending'; BREAK the batch
  `-- permanent error   -> status='failed', failed_at=now(), last_error
                           CONTINUE the batch
```

then commits.

Error classification, the one piece of judgement in the relay:

- Retryable: `redis.exceptions.ConnectionError`, `TimeoutError`,
  `BusyLoadingError`, `ReadOnlyError`. Redis is down or unavailable; the event
  is fine and must eventually go out. Breaking the batch is deliberate — if
  Redis is unreachable for one row it is unreachable for all of them, and
  stopping preserves publish order.
- Permanent: `redis.exceptions.ResponseError` and any other non-transport
  exception. `WRONGTYPE` because the stream key was clobbered, or a payload
  over `proto-max-bulk-len`, will never succeed on retry. Continuing the batch
  is equally deliberate: one unpublishable row must not block the rows behind
  it. Without this branch, break-on-failure would head-of-line block forever.
- Local integrity failure (`payload` NULL or empty) is treated as permanent.
  It should be unreachable; it exists so corruption surfaces as a `failed` row
  rather than a crashed relay.

Backoff is exponential from `OUTBOX_RETRY_BACKOFF_MS`, capped at
`OUTBOX_MAX_BACKOFF_MS`.

One transaction covers the whole batch: the claim, every `XADD`, and every
mark. This is an accepted tradeoff, and the README records its two
consequences.

The first is duplicate amplification. A crash after publishing some rows but
before the batch commits rolls back every `published` mark in that batch, so
all the already-published rows are published again on restart. The window is
bounded by `OUTBOX_BATCH_SIZE`, which is the operator's lever: smaller batches
duplicate less and cost more transactions.

The second is transaction duration. The batch's `XADD` calls happen inside the
open transaction, so a slow Redis holds a pooled connection — out of
`pool_size=5, max_overflow=10`, shared with request handling — and holds back
`VACUUM`'s cleanup horizon for as long as the batch runs. Lock contention is
not the cost here: nothing contends for outbox rows except other relays, and
`SKIP LOCKED` sends them straight past.

Per-row transactions would cut the duplicate window to one event, but the
batch claim is one round trip and the marks are one bulk `UPDATE` — about
three round trips for a whole batch against two per row. The batch keeps that
margin; `OUTBOX_BATCH_SIZE` trades it back when an operator wants a narrower
window.

Retention sweep runs on an `OUTBOX_SWEEP_INTERVAL_S` timer inside the same
loop — no third background task:

```sql
DELETE FROM outbox
WHERE status = 'published' AND published_at < now() - :retention
```

`failed` rows are never swept; they are the dead-letter record and are removed
by an operator after investigation.

### `core/services/order_service.py`

`create_order` becomes: check for a duplicate, create the order row, build the
envelope, `OutboxRepository.add(envelope)`, one `commit()`, then
`get_relay().notify()`.

The `try/except Exception` around the publish and the docstring paragraph
about non-atomic sequencing are both deleted — there is no publish here that
can fail. The `IntegrityError` -> `DuplicateOrderError` mapping stays as is.

Interface changes:

- `OrderService.__init__` drops its `producer` parameter.
- `create_order` returns `Order` instead of `tuple[Order, str | None]`; no
  message id exists at request time.

### `api/routes/orders.py`

Unpacks a single `Order`, and drops the `if message_id is None` warning
branch. The 201 response still reports `status: "pending"` — unchanged, since
that has always been the consumer's state to advance. The docstring is
rewritten to say the event is written to the outbox rather than published.

### `main.py`

Constructs the relay alongside the consumer, starts it as a second task with
the same `add_done_callback` error logging, and on shutdown stops it with the
same `wait_for` / cancel pattern before `close_producer()` and `close_db()`.
Skipped entirely when `RELAY_ENABLED` is false.

### `config/settings.py`

A new "Outbox relay" block:

| setting | default | meaning |
| --- | --- | --- |
| `RELAY_ENABLED` | `True` | start the relay in this process |
| `OUTBOX_POLL_INTERVAL_MS` | `200` | idle wait when the nudge does not fire |
| `OUTBOX_BATCH_SIZE` | `20` | rows claimed per transaction; also caps duplicate amplification |
| `OUTBOX_RETRY_BACKOFF_MS` | `500` | base transport backoff |
| `OUTBOX_MAX_BACKOFF_MS` | `30_000` | backoff cap |
| `OUTBOX_RETENTION_HOURS` | `24` | age at which published rows are swept |
| `OUTBOX_SWEEP_INTERVAL_S` | `300` | how often the sweep runs |

`.env.example` gains the same keys.

## Testing

Unit (`make test`, no Docker):

- `create_order` writes the order row and the outbox row in one transaction
  and commits exactly once, with no Redis interaction on the request path.
  `OrderService` no longer has a producer to assert against, so the test
  patches `get_producer` to raise and passes only if it is never called.
- `create_order` calls `notify()` after a successful commit and not after a
  `DuplicateOrderError`.
- Error classification: a fake producer raising `ConnectionError` leaves the
  row `pending` with `attempts` incremented; one raising `ResponseError` marks
  it `failed` with `failed_at` set.
- Backoff arithmetic, including the cap.
- The relay stops the batch on a transport error and continues it on a
  permanent one.

Integration (`make test-all`, testcontainers):

- A pending row is published and marked `published`, and the envelope observed
  on the stream is byte-identical to `payload`.
- Redis unreachable: the row stays `pending` with `attempts` incremented, then
  publishes once Redis returns.
- A row with a corrupt `payload` is marked `failed` and the relay keeps
  draining the rest of the batch.
- Two concurrent relays using `SKIP LOCKED` never claim the same row; every
  pending row is published once in the no-failure case. This test demonstrates
  non-overlapping claims, not an end-to-end exactly-once guarantee — the
  system is at-least-once, and the test name says so.
- The sweep deletes aged `published` rows and leaves `failed` and `pending`
  rows alone.
- `test_order_roundtrip.py` flows through the relay: POST, wait, GET
  `confirmed`.
- `test_migrations.py` needs no change; it already covers the new revision's
  `downgrade()`.

## Documentation

`README.md`: the "Known limitation: no transactional outbox" section is
replaced by an "Outbox" section covering the write path, the relay loop, the
settings, and this residual-limitations list:

- Delivery is at-least-once. A crash between `XADD` and the marking commit
  republishes the row; handlers must be idempotent.
- A relay transaction covers an entire claimed batch, so that crash
  republishes not just one row but every row already published in the batch.
  Lower `OUTBOX_BATCH_SIZE` to narrow the window, at the cost of more database
  transactions.
- The batch's `XADD` calls run inside the open transaction, so a slow Redis
  keeps a pooled connection checked out and delays `VACUUM` cleanup for the
  duration of the batch.
- Publish order is not guaranteed globally when more than one replica runs a
  relay. `SKIP LOCKED` lets a later row overtake an earlier one held by
  another relay. Per-aggregate ordering needs partitioning by
  `correlation_id`, which this template does not do.
- The relay shares the API's process and event loop. Splitting it into its own
  container is a deployment change, not a code change: it is already a
  self-contained module behind `RELAY_ENABLED`.

`CLAUDE.md`: an "Outbox" section alongside "Consumer", and the Architecture
section's messaging bullet gains `messaging/outbox/relay.py`.

## Out of scope

- A separate relay container or entrypoint.
- `LISTEN`/`NOTIFY`-driven wakeups.
- Ordering guarantees across replicas.
- An admin endpoint for inspecting or replaying `failed` rows.
