# Python Microservice Template

An async FastAPI microservice template with a working reference slice: an
`orders` API backed by PostgreSQL, publishing and consuming events over Redis
Streams. Read it end to end, then replace the slice with your own domain.

---

## Quickstart

```bash
make up      # docker compose up --build -d; runs migrations, starts the API
make demo    # POST an order, wait for the consumer, GET it back
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

## Known limitation: no transactional outbox

The database commit and the `XADD` publish are two separate operations, not
one atomic unit. If the commit succeeds but the publish fails, the API still
returns `201` with `status: "pending"` — the row is real, the event is not,
and the response reflects that truthfully rather than pretending otherwise.

A production service closes this gap with a transactional outbox: write the
event to an outbox table in the same transaction as the domain row, then have
a separate relay process poll the outbox and publish, marking rows as sent.
This template omits it deliberately — it adds a second table and a second
background loop, roughly doubling the code for a reference slice whose job is
to demonstrate the event flow clearly, not to be production-hardened.

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
