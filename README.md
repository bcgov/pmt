# Python Microservice Template

An async FastAPI microservice template with a working reference slice: an
`orders` API backed by PostgreSQL, publishing and consuming events over Redis
Streams, and priced from a catalog in S3-compatible object storage
(SeaweedFS in this template). Read it end to end, then replace the slice with
your own domain.

---

## Quickstart

```bash
make up      # docker compose up --build -d; runs migrations, starts the API
make demo    # POST an order, wait for the consumer, GET it back
make demo-s3 # same, but priced from S3 and rolled up into a daily object
```

Expected output:

```
--- POST /orders
{
    "order_ref": "demo-1",
    "item": "widget",
    "quantity": 3,
    "status": "pending",
    ...
}
--- waiting for the consumer...
--- GET /orders/demo-1
{
    "order_ref": "demo-1",
    "item": "widget",
    "quantity": 3,
    "status": "confirmed",
    ...
}
```

The order starts `pending` and is `confirmed` by the time you fetch it.
Interactive API docs are at [http://localhost:8000/docs](http://localhost:8000/docs).

---

## What the demo does

```
POST /orders {order_ref, item, quantity}
  └→ INSERT orders (status=pending)          [request-scoped session]
  └→ XADD order.events "OrderCreated"
                                             → 201 {status: "pending"}

[consumer] XREADGROUP
  └→ handler: UPDATE orders
       SET status='confirmed', confirmed_at=now()
       WHERE order_ref=:ref AND status='pending'   [own session]
  └→ XACK

GET /orders/{order_ref} → {status: "confirmed"}
```

The API writes the row and publishes an event in the same request; the
consumer, running as a background task inside the same process, reads that
event and moves the order to `confirmed`. Nothing in the API itself sets
`confirmed` — that only happens on the consumer side, so you're watching a
real asynchronous handoff, not a synchronous illusion.

---

## Make it yours

The `orders` slice exists to be replaced. Edit these seven files, in order:

1. `db/models.py` — your entity, in place of `Order`.
2. `db/repositories/order_repository.py` — your queries.
3. `messaging/models/events/order_created.py` — your event payload.
4. `messaging/models/envelope.py` — add your event type to the `Literal` and
   to `EventPayload`.
5. `messaging/consumer/handlers/order_created.py` — your handler.
6. `messaging/consumer/dispatcher.py` — register the handler in `HANDLERS`.
7. `core/services/order_service.py` and `api/routes/orders.py` — your service
   and routes.

`storage/` (the S3 object-storage layer — price catalog, rollups) is part of
the reference slice, not core infrastructure: keep it if your domain also
reads or writes object storage, delete it along with SeaweedFS in
`docker-compose.yaml` otherwise.

Then generate and apply a migration for your table:

```bash
make revision m="create <your table>"
make migrate
```

---

## Migrations

Schema changes go through Alembic only — there is no `create_all` anywhere in
this codebase, and the app does not create tables on startup. The schema has
exactly one source of truth: the revisions in `db/migrations/versions/`.

```bash
make revision m="add a column"   # autogenerate a revision from your models
make migrate                      # alembic upgrade head
```

The compose entrypoint runs `alembic upgrade head` before starting uvicorn, so
`make up` always leaves the database at the latest revision.

---

## Reliability: retry, DLQ, and crash recovery

The consumer (`messaging/consumer/redis_consumer.py`) is at-least-once, not
exactly-once, and handles failure explicitly:

- A handler that raises is retried in-process up to `CONSUMER_MAX_RETRIES`
  times, with exponential backoff (`CONSUMER_RETRY_BACKOFF_MS`).
- After retries are exhausted, the message is written to the dead-letter
  stream (`DLQ_STREAM_NAME`, default `<STREAM_NAME>:dlq`) with its error,
  traceback, delivery count, and failure time — then acked, so it leaves the
  pending list.
- An envelope that fails validation goes straight to the DLQ with no retries:
  a message that can't parse will never parse.
- `XAUTOCLAIM` runs on startup and whenever a poll comes back empty, reclaiming
  messages orphaned by a killed consumer process. A reclaimed message whose
  delivery count already exceeds `CONSUMER_MAX_RETRIES` is dead-lettered
  immediately rather than retried again — otherwise a message that crashes the
  process gets reclaimed and crashes it forever.

To inspect the DLQ:

```bash
docker compose exec redis redis-cli -a redis XRANGE order.events:dlq - +
```

Because delivery is at-least-once, handlers must be idempotent. The order
handler's update is conditional
(`WHERE order_ref=:ref AND status='pending'`); a redelivered message affects
zero rows, logs it, and acks normally.

---

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

---

## Storage

The `OrderCreated` handler prices the order from a catalog in S3-compatible
object storage, then rebuilds a daily rollup object — `storage/` (beside
`db/` and `messaging/`) is the layer that does that. The HTTP write path
never touches S3; only the consumer does.

`docker-compose.yaml` runs SeaweedFS as that object store, with a
`bucket-init` service that creates `pmt-bucket` and seeds
`config/prices.json` so the first order has something to price against.

Settings: `S3_ENDPOINT_URL`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`,
`S3_SECRET_ACCESS_KEY`, `S3_PRICES_KEY` (default `config/prices.json`),
`S3_ROLLUP_PREFIX` (default `rollups/`), `S3_CONNECT_TIMEOUT_S` (default `2`),
`S3_READ_TIMEOUT_S` (default `5`), `PRICES_CACHE_TTL_S` — how long the
parsed catalog is cached before revalidating with `If-None-Match`.

Run `make demo-s3` to watch it end to end: the seeded catalog, an order
priced from it, and the rollup object the consumer wrote.

---

## Testing

```bash
make test       # poetry run pytest -m "not integration" — no Docker required
make test-all   # poetry run pytest — includes testcontainers-backed integration tests
```

`make test` runs the unit suite (service sequencing, dispatcher routing,
handler logic) with everything mocked, so it works without Docker. `make
test-all` additionally spins up real Postgres and Redis via testcontainers and
runs the round-trip test (`POST /orders` → poll `GET` until `confirmed`), a
DLQ test, and a migration up/down/up test — this is the suite that actually
proves the template works.
