# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python microservice template built with FastAPI, using async/await
throughout. It ships a working reference vertical slice — an `orders` API
backed by PostgreSQL, publishing and consuming events over Redis Streams —
that a real service is meant to replace. See README.md for the quickstart and
the "Make it yours" checklist of files to edit.

## Development Commands

Use the Makefile targets; they wrap the underlying commands and are what the
README's quickstart depends on.

```bash
make up          # docker compose up --build -d (runs migrations, starts the API)
make down        # docker compose down
make logs        # tail the api container's logs
make demo        # POST an order, wait for the consumer, GET it back
make migrate     # alembic upgrade head, against the running database
make revision m="add a column"   # alembic revision --autogenerate
make test        # poetry run pytest -m "not integration" — no Docker required
make test-all    # poetry run pytest — includes testcontainers integration tests
make lint        # poetry run ruff check .
make fmt         # poetry run black . && poetry run ruff check --fix .
```

### Running the application locally (without Docker)

```bash
uvicorn main:app --host 0.0.0.0 --port 8099 --reload
# or
python main.py
```

`main.py` defaults to port **8099** when run locally via `python main.py` or
plain `uvicorn`. Docker Compose overrides this: it sets `SERVICE_PORT=8000`
and maps `8000:8000`, so the app under `make up` is at
`http://localhost:8000`, not 8099. Note that startup does not create the
schema — migrations must be applied first (`make migrate`, or the compose
entrypoint, which runs it automatically).

### Dependency Management

```bash
poetry install
poetry add <package-name>
poetry add --group dev <package-name>
```

## Architecture

### Layered Structure

1. **API Layer** (`api/`): FastAPI routers and Pydantic request/response
   models.
   - `api/routes/`: `health`, `info`, `orders`.
   - `api/routes/models.py`: `CreateOrderRequest`, `OrderResponse`.

2. **Core Layer** (`core/services/`): Application services that sequence data
   and messaging work. `order_service.py` writes the order row and its
   `OrderCreated` event to the `outbox` table in one transaction and never
   publishes; the relay does that afterwards.

3. **Data Access Layer** (`db/`): PostgreSQL via SQLAlchemy 2.0 async.
   - `db/postgres/session.py`: async engine, session maker, `get_db()`
     dependency. Does **not** create tables — that's Alembic's job.
   - `db/models.py`: ORM models (`Order`).
   - `db/repositories/`: query logic (`order_repository.py`,
     `outbox_repository.py`).
   - `db/migrations/`: Alembic environment and revisions — see Migrations
     below.

4. **Messaging Layer** (`messaging/`): Redis Streams, fully async.
   - `messaging/producer/redis_producer.py`: `XADD`, exposed via
     `get_producer()`.
   - `messaging/consumer/redis_consumer.py`: `RedisConsumer` — async
     `XREADGROUP` loop with retry, dead-lettering, and `XAUTOCLAIM` recovery.
   - `messaging/consumer/dispatcher.py`: routes envelopes to handlers via the
     `HANDLERS` registry.
   - `messaging/consumer/handlers/`: one handler per event type
     (`order_created.py`).
   - `messaging/models/`: `EventEnvelope`, `EventPayload`, and per-event
     payload models (`events/order_created.py`).
   - `messaging/outbox/relay.py`: `OutboxRelay` — claims `outbox` rows with
     `SKIP LOCKED` and publishes them; `messaging/outbox/backoff.py` classifies
     Redis errors as retryable or permanent.

5. **Configuration Layer** (`config/`):
   - `settings.py`: Pydantic Settings for env var validation.
   - `logging.py`: structlog with OpenTelemetry trace context.
   - `tracing.py`: OpenTelemetry instrumentation.
   - `request_logger.py`: HTTP request/response logging middleware.

There is no caching layer — `cache/` does not exist, and no settings reference
Redis for caching (only `REDIS_STREAM_URL`, for messaging).

### Application Entry Point

`main.py`:
- Configures logging and tracing on import.
- Adds `RequestLoggingMiddleware`.
- Starts `RedisConsumer` as a background asyncio task in the lifespan
  (started on startup, stopped — with a timeout before cancellation — on
  shutdown).
- Includes routers from `api/routes/` (`health`, `info`, `orders`).
- Provides a `main()` function that runs uvicorn on port 8099 for local runs.

### Configuration System

Settings are managed via Pydantic Settings (`config/settings.py`):
- Loads from `.env` (use `.env.example` as a template).
- Use `get_settings()` (cached via `@lru_cache`) to access settings.
- Key settings:
  - `DATABASE_URL`: PostgreSQL connection string.
  - `REDIS_STREAM_URL`: Redis instance for Streams messaging.
  - `STREAM_NAME` / `CONSUMER_GROUP`: `order.events` / `order_service_v1` by
    default.
  - `CONSUMER_MAX_RETRIES`, `CONSUMER_RETRY_BACKOFF_MS`,
    `CONSUMER_CLAIM_MIN_IDLE_MS`, `CONSUMER_BATCH_SIZE`, `DLQ_STREAM_NAME`:
    consumer reliability tuning (see Consumer below).
  - `LOG_LEVEL`, `JSON_LOGS`: logging configuration.
  - `ENVIRONMENT`: "development", "staging", or "production".

### Observability

1. **Structured Logging** (config/logging.py): structlog, with automatic
   OpenTelemetry trace context (trace_id, span_id). Get a logger via
   `from config.logging import get_logger; logger = get_logger(__name__)`.
2. **Distributed Tracing** (config/tracing.py): OpenTelemetry with OTLP HTTP
   export and FastAPI auto-instrumentation.
3. **Request Logging** (config/request_logger.py): logs method, path, status,
   and duration for every HTTP request.

### Router Pattern

- Each router in `api/routes/` defines its own prefix and tags.
- Routers are included in `main.py` via `app.include_router()`.
- Example: `orders.router` provides `POST /orders` and `GET /orders/{order_ref}`.

### Health Check Pattern

`api/routes/health.py` implements both dependency probes (not stubbed out):
Postgres via `SELECT 1` and Redis via `PING`, each with a 2-second timeout.
Any failed probe returns HTTP 503 with per-dependency status in the body.

## Migrations

Schema changes go through Alembic exclusively. There is no `create_all` call
anywhere — `db/postgres/session.py` builds the engine and session maker only;
it does not create tables. The schema has exactly one source of truth: the
checked-in revisions in `db/migrations/versions/`.

- `db/migrations/env.py` builds an async engine from `DATABASE_URL` (via
  `get_settings()`) and imports `db.models` so `target_metadata` covers every
  model — `alembic revision --autogenerate` works without extra wiring.
- Generate a revision after changing `db/models.py`:
  `make revision m="describe the change"`.
- Apply pending revisions: `make migrate` (`alembic upgrade head`).
- The compose entrypoint runs `alembic upgrade head` before starting uvicorn,
  so `make up` always leaves the schema current.
- Every revision must implement `downgrade()` — `tests/integration/test_migrations.py`
  exercises `upgrade head` → `downgrade base` → `upgrade head` to catch
  revisions that don't.

## Consumer

`messaging/consumer/redis_consumer.py` (`RedisConsumer`) uses the async Redis
client (`redis.asyncio.Redis`) exclusively — a synchronous client here would
block the event loop that also serves HTTP.

- Handlers must be idempotent: Redis Streams delivery is at-least-once, and
  redelivery is a normal occurrence, not a bug. The order handler achieves
  this with a conditional `UPDATE ... WHERE status='pending'`; a redelivered
  message affects zero rows, logs "already processed," and acks.
- On handler failure, the message is retried in-process up to
  `CONSUMER_MAX_RETRIES` times with exponential backoff
  (`CONSUMER_RETRY_BACKOFF_MS`), then dead-lettered to `DLQ_STREAM_NAME`
  (default `<STREAM_NAME>:dlq`) with its error, traceback, delivery count, and
  failure time, and acked so it leaves the pending list.
- An envelope that fails Pydantic validation is dead-lettered immediately,
  with no retries.
- `XAUTOCLAIM` runs on `start()` and whenever a poll returns nothing, reclaiming
  messages orphaned by a crashed consumer. A reclaimed message already past
  `CONSUMER_MAX_RETRIES` deliveries is dead-lettered directly rather than
  retried again (the poison-message guard).
- New event types: add the payload model under `messaging/models/events/`,
  add the type to the `Literal` and `EventPayload` union in
  `messaging/models/envelope.py`, write a handler under
  `messaging/consumer/handlers/`, and register it in `HANDLERS` in
  `messaging/consumer/dispatcher.py`.
- A handler raising `PermanentHandlerError` (`storage/errors.py`) is
  dead-lettered immediately with reason `permanent_handler_error`, no retries —
  the same treatment a `ValidationError` gets, for the same reason.

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

## Storage

`storage/` is the object-storage layer, beside `db/` and `messaging/`. The
`OrderCreated` handler reads a price catalog from S3, writes the total to
Postgres, and rebuilds a daily rollup object — the HTTP write path never
touches S3.

- `storage/object_store.py` defines the `ObjectStore` Protocol. Depend on it,
  not on `aioboto3`; that is what lets every unit test run against
  `storage/fake.py` with no Docker.
- `storage/s3/client.py` holds one long-lived `aioboto3` client, opened in the
  lifespan. botocore's own retries are disabled — retry policy lives in the
  consumer, and two layers would multiply.
- `storage/errors.py` classifies failures the way `messaging/outbox/backoff.py`
  does for Redis, with one difference: `ClientError` covers both a 503 and a
  `NoSuchKey`, so it branches on the response code, not the exception class.
  A `PermanentHandlerError` is dead-lettered by the consumer with no retries.
- **Money is integers.** `unit_price_cents` in the catalog, `total_cents
  BIGINT` in Postgres, `total_cents` in the API response. One representation,
  no conversion boundary. `PriceItem.unit_price_cents` is a `StrictInt`, so a
  `19.99` in the catalog is a validation failure rather than a truncation.
- **The rollup object is a projection, never an accumulator.**
  `RollupService.rebuild_for_date` selects the day from SQL and overwrites the
  key; it never reads what is there. Concurrent writers race harmlessly
  because they all compute from the same authoritative rows. Do not turn this
  into a read-modify-write — that needs conditional PUTs and a retry loop to
  avoid losing updates.
- **The rollup is rebuilt on every delivery, including redeliveries** whose
  `confirm()` affected zero rows. Skipping it looks like an optimization and
  breaks recovery: a PUT that failed after a successful commit would never be
  retried, because the retry's `confirm()` is a no-op.
- `PriceCatalog` caches the parsed catalog for `PRICES_CACHE_TTL_S` and then
  revalidates with `If-None-Match`. A failed revalidation propagates without
  discarding the cached copy.

## Code Style

- Line length: 88 characters (Black default).
- Ruff selects: E, F, W, B, I (with E501 ignored since Black handles line length).
- Import order: standard library, third-party, local.
