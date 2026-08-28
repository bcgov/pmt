# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python Redis Streams worker template, using async/await throughout.
It ships a working reference pipeline that a real service is meant to replace:
a CLI publishes an `OrderCreated` event, the consumer computes a total,
publishes `OrderConfirmed`, and consumes that too — all under one distributed
trace. There is no HTTP API and no database. See README.md for the quickstart
and the "Make it yours" checklist of files to edit.

## Development Commands

Use the Makefile targets; they wrap the underlying commands and are what the
README's quickstart depends on.

```bash
make up          # docker compose up --build -d (starts Redis and the worker)
make down        # docker compose down
make logs        # tail the worker container's logs
make health      # curl the worker's health endpoint
make demo        # publish an order twice, watch it confirm once
make test        # poetry run pytest -m "not integration" — no Docker required
make test-all    # poetry run pytest — includes testcontainers integration tests
make lint        # poetry run ruff check .
make fmt         # poetry run black . && poetry run ruff check --fix .
```

### Running the application locally (without Docker)

```bash
python main.py                                    # the worker
python -m cli publish --ref demo-1 --item widget \
                      --quantity 3 --unit-price-cents 450 --count 2
```

No configuration is required: every setting has a default, and the defaults
point at a local Redis (`redis://localhost:6379/1`). `HEALTH_PORT` defaults to
8000, so the worker's probe is at `http://localhost:8000/health` both locally
and under `make up`.

### Dependency Management

```bash
poetry install
poetry add <package-name>
poetry add --group dev <package-name>
```

## Architecture

### Layered Structure

1. **Messaging Layer** (`messaging/`): Redis Streams, fully async.
   - `messaging/producer/redis_producer.py`: `XADD`, exposed via
     `get_producer()`. Injects the current trace context into the envelope.
   - `messaging/consumer/redis_consumer.py`: `RedisConsumer` — async
     `XREADGROUP` loop with retry, dead-lettering, and `XAUTOCLAIM` recovery.
   - `messaging/consumer/dispatcher.py`: routes envelopes to handlers via the
     `HANDLERS` registry.
   - `messaging/consumer/handlers/`: one handler per event type
     (`order_created.py`, `order_confirmed.py`).
   - `messaging/models/`: `EventEnvelope`, `EventPayload`, and per-event
     payload models (`events/order_created.py`, `events/order_confirmed.py`).
   - `messaging/state.py`: the handler-side Redis client
     (`get_state_client()` / `close_state_client()`), deliberately separate
     from the consumer's connection.

2. **Configuration Layer** (`config/`):
   - `settings.py`: Pydantic Settings for env var validation.
   - `logging.py`: structlog with OpenTelemetry trace context.
   - `tracing.py`: OpenTelemetry instrumentation.

Alongside those:
   - `health/server.py`: a stdlib `asyncio.start_server` probe endpoint.
   - `cli.py`: the one-shot event publisher (`python -m cli publish ...`).
   - `money.py`: `format_cents()`, the only place cents become a string.

There is no API layer, no service layer, and no database layer — those were
removed, not replaced.

### Application Entry Point

`main.py`:
- Configures logging and tracing on import.
- `run_worker()` starts `RedisConsumer` and the health server on one event
  loop and installs SIGINT/SIGTERM handlers that set the stop event.
- On shutdown it calls `consumer.stop()`, waits up to
  `SHUTDOWN_TIMEOUT_SECONDS` for the in-flight message (cancelling the task if
  that deadline passes), then closes the health server and the consumer,
  producer and state clients.
- Provides a `main()` function that runs `run_worker()` under `asyncio.run`.

### Configuration System

Settings are managed via Pydantic Settings (`config/settings.py`):
- Loads from `.env` (use `.env.example` as a template).
- Use `get_settings()` (cached via `@lru_cache`) to access settings.
- There are **no required settings** — every field has a default.
- Key settings:
  - `REDIS_STREAM_URL`: Redis instance for Streams messaging and handler state.
  - `STREAM_NAME` / `CONSUMER_GROUP`: `order.events` / `order_service_v1` by
    default.
  - `HEALTH_PORT`: port for the health probe server (8000).
  - `STATE_TTL_SECONDS`: TTL on the handler's `order:*` state keys (3600).
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
   export. There is no FastAPI auto-instrumentation any more — every span is
   created explicitly by the producer, the consumer or the CLI.
3. **Trace propagation**: context crosses the stream in
   `EventEnvelope.traceparent`. The producer injects from whatever span is
   current; the consumer extracts it and makes its message span current with
   `start_as_current_span`. That "current" part is load-bearing: a detached
   span exports identically and silently orphans everything a handler
   publishes.

### Health Check Pattern

`health/server.py` serves exactly one route, `/health`, on a stdlib asyncio
server — no ASGI framework. It probes Redis with `PING` under a 2-second
timeout and returns 503 with `{"status": "degraded", "redis": "down"}` when
that fails, 200 with `{"status": "ok", "redis": "ok"}` otherwise. Any other
path is a 404.

## Consumer

`messaging/consumer/redis_consumer.py` (`RedisConsumer`) uses the async Redis
client (`redis.asyncio.Redis`) exclusively — a synchronous client here would
block the event loop the health server also runs on.

- Handlers must be idempotent: Redis Streams delivery is at-least-once, and
  redelivery is a normal occurrence, not a bug. The `OrderCreated` handler
  achieves this with `HSETNX order:<ref> status confirmed`; a redelivered
  message sees 0, logs "already processed," and acks.
- A redelivery must not publish a downstream event. The early return sits
  *before* the publish — correct local state is only half of idempotency; the
  other half is not amplifying one redelivery through every consumer
  downstream.
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
- New event types are a four-file edit, in order:
  1. `messaging/models/events/` — the payload model.
  2. `messaging/models/envelope.py` — add the type to the `Literal` and to the
     `EventPayload` union.
  3. `messaging/consumer/handlers/` — the handler.
  4. `messaging/consumer/dispatcher.py` — register it in `HANDLERS`.

  `OrderConfirmed` is a worked example of exactly that edit. Do not publish
  upstream from a handler: both sample events share one stream, so a handler
  that republishes what it consumes is an infinite loop.

## Code Style

- Line length: 88 characters (Black default).
- Ruff selects: E, F, W, B, I (with E501 ignored since Black handles line length).
- Import order: standard library, third-party, local.

### Money

Amounts are integer minor units everywhere: `1000` means `10.00`. Fields carry
a `_cents` suffix (`unit_price_cents`, `total_cents`). Never `float`, never
`Decimal`. Formatting happens only at output edges — log lines and CLI output
— via `format_cents()` in `money.py`, so the wire format never depends on
presentation.
