# Redis-Only Template — Design

Date: 2026-08-27
Branch: `redis-only-template`
Status: approved, ready for implementation planning

## Goal

Reduce the microservice template to one thing: a Redis Streams producer and
consumer. The HTTP API and PostgreSQL layers are removed entirely. What
remains is a worker process that consumes events, a CLI that publishes them,
and the envelope/dispatcher/handler machinery between them.

The template's teaching value must survive the strip. Specifically: at-least-once
delivery, idempotent handlers, retry, dead-lettering, `XAUTOCLAIM` recovery,
and a validated envelope as the single schema boundary.

## Non-goals

- No HTTP API, no request/response layer, no OpenAPI docs.
- No relational database, no ORM, no migrations.
- No new domain. The `OrderCreated` sample event is retained as-is; only its
  handler's storage changes.

## Architecture

Two processes over one Docker image:

- **Worker** (`main.py`) — long-running. Runs the `RedisConsumer` loop and a
  minimal health server concurrently on one event loop.
- **CLI** (`cli.py`) — one-shot. Builds an `EventEnvelope` and publishes it via
  the existing `RedisProducer`.

Redis serves three roles: the event stream, the dead-letter stream, and the
handler's state store. No other infrastructure.

### Package layout

Existing flat top-level packages are retained (`messaging/`, `config/`); no
imports move. Two additions:

- `health/server.py` — the stdlib asyncio health server.
- `cli.py` — the publisher CLI, at the repo root, invoked `python -m cli`.

## Component design

### 1. Entry point (`main.py`)

Replaces the FastAPI app. Responsibilities:

- `configure_logging()` and `init_tracing()` on import, as today.
- `asyncio.run()` over `gather(consumer.start(), serve_health())`.
- Install SIGINT/SIGTERM handlers that trigger the same graceful shutdown the
  current lifespan performs: `consumer.stop()`, await the task with a 10s
  timeout before cancelling, `consumer.close()`, `close_producer()`.
- No `close_db()` — `db/` is gone.

### 2. Health server (`health/server.py`)

`asyncio.start_server` on `settings.HEALTH_PORT`. Reads the request line,
ignores everything else, and responds:

- `GET /health` → Redis `PING` with a 2-second timeout. `200` with
  `{"status":"ok","redis":"ok"}`, or `503` with `{"status":"degraded","redis":"down"}`.
- Any other path → `404`.

Connection: `Connection: close` on every response; no keep-alive handling. This
is a probe endpoint, not a web framework, and the code should say so in a
docstring.

### 3. CLI (`cli.py`)

`argparse`, no new dependency.

```
python -m cli publish --ref demo-1 --item widget --quantity 3 [--count N]
```

Builds `EventEnvelope.create(event_type="OrderCreated", ..., source="cli")` and
publishes each one, printing the returned stream message id. `--count` repeats
the same ref, which is what makes the idempotency demo visible. Closes the
producer before exiting.

### 4. Handler (`messaging/consumer/handlers/order_created.py`)

Replaces the SQL `UPDATE ... WHERE status='pending'` with a conditional Redis
write that preserves the same semantics.

```python
key = f"order:{payload.order_ref}"
first = await r.hsetnx(key, "status", "confirmed")
if not first:
    log.info("Order already processed; nothing to do")
    return
await r.hset(key, mapping={"item": ..., "quantity": ..., "confirmed_at": ...})
await r.expire(key, settings.STATE_TTL_SECONDS)
log.info("Order confirmed")
```

The handler owns its own Redis client, distinct from the consumer's — this
mirrors, and the docstring should carry over, the current lesson that a handler
has no ambient request context and must acquire its own resources.

Known non-atomicity, to be documented in the docstring rather than engineered
around: `HSETNX` followed by `EXPIRE` is two round trips, so a crash between
them leaves a key with no TTL. Acceptable for a demo store; the comment names
a Lua script or `SET NX` as the atomic alternative.

### 5. Tracing (`config/tracing.py`, envelope, producer, consumer)

FastAPI auto-instrumentation is removed, so trace continuity becomes explicit:

- `EventEnvelope` gains `traceparent: str | None = None`.
- `RedisProducer.publish` injects the current context via the OTel W3C
  propagator into that field.
- `RedisConsumer` extracts it and opens the per-message span as a child of the
  publishing span.

`extra="forbid"` on the envelope means an event published before this field
existed still validates (the field is optional, not required), but an envelope
carrying an unknown field does not — unchanged behaviour.

### 6. Settings (`config/settings.py`)

- Removed: `DATABASE_URL`. It is the only required field today; without it the
  template runs with zero configuration.
- Added: `HEALTH_PORT: int = 8000`, `STATE_TTL_SECONDS: int = 3600`.
- Retained unchanged: all `REDIS_*`, `STREAM_*`, `CONSUMER_*`, `DLQ_*`,
  logging, and OTEL settings.
- `SERVICE_PORT` is removed in favour of `HEALTH_PORT`, which names what it
  actually binds.

## Deletions

Trees: `api/`, `core/`, `db/`.
Files: `alembic.ini`, `docker-entrypoint.sh`, `config/request_logger.py`.
Dependencies: `fastapi`, `uvicorn`, `sqlalchemy`, `asyncpg`, `alembic`,
`testcontainers` Postgres extra.
Compose: the `db` service, the `pmt_pgdata` volume, `DATABASE_URL`.
Makefile: `migrate`, `revision`; `up`/`demo` rewritten.
Dockerfile: entrypoint becomes `CMD ["python", "main.py"]`.

## Data flow

```
$ make demo
  python -m cli publish --ref demo-1 --count 2
    └→ XADD order.events {"event": "<envelope json>"}   x2

[worker] XREADGROUP
  └→ envelope validated → dispatch → handler
       HSETNX order:demo-1 status confirmed  → 1 → "Order confirmed"
  └→ XACK
[worker] XREADGROUP  (the second copy)
  └→ HSETNX order:demo-1 status confirmed  → 0 → "Order already processed"
  └→ XACK

$ redis-cli HGETALL order:demo-1
```

## Error handling

Unchanged from the current consumer, and this is deliberate — the reliability
machinery is the point of the template:

- Envelope validation failure → dead-letter immediately, no retries.
- Handler exception → retry up to `CONSUMER_MAX_RETRIES` with exponential
  backoff, then dead-letter with error, traceback, delivery count, and failure
  time; ack either way.
- `XAUTOCLAIM` on start and on every empty poll; a reclaimed message already
  past the retry ceiling is dead-lettered directly.
- CLI publish failure → non-zero exit with the error printed. There is no row
  to be inconsistent with any more, so the "committed but unpublished" caveat
  that `OrderService` documented disappears along with the service.

## Testing

`tests/conftest.py` loses the Postgres container, `migrated_db`,
`reset_engine_globals`, `_reset_session_maker_globals`, and `db_session` — a
substantial simplification. The Redis container fixture and `app_settings`
remain.

- Deleted: `test_migrations.py`, `test_order_repository.py`,
  `test_order_routes.py`, `test_order_service.py`.
- Rewritten: `test_health.py` (health server against reachable and unreachable
  Redis), `test_order_created_handler.py` (fresh key confirms; replay is a
  no-op and leaves the hash unchanged), `test_order_roundtrip.py` (CLI publish
  → consumer → key state), `test_main.py` (worker starts both tasks; signal
  triggers graceful shutdown).
- Unchanged: `test_dispatcher.py`, `test_envelope.py`, `test_redis_consumer.py`,
  `test_producer.py`. `test_envelope.py` gains a case for `traceparent`.

The unit/integration marker split and the `make test` / `make test-all`
targets are unchanged.

## Documentation

- **README.md** — rewritten around the worker and CLI. The quickstart becomes
  `make up` / `make demo`; the expected output is worker log lines and a
  `HGETALL`, not JSON responses. "Make it yours" shrinks from seven files to
  four: `messaging/models/events/order_created.py`, `messaging/models/envelope.py`,
  `messaging/consumer/handlers/`, `messaging/consumer/dispatcher.py`.
- **CLAUDE.md** — Architecture loses layers 1–3 (API, Core, Data Access) and
  gains the worker/CLI entry points; the Migrations section is deleted whole;
  Development Commands loses `migrate` and `revision`. The Consumer section
  survives nearly intact, with the idempotency example restated in terms of
  `HSETNX`.
- **.env.example** — `DATABASE_URL` removed, `HEALTH_PORT` and
  `STATE_TTL_SECONDS` added.

## Open risk

The tracing work (§5) is the only genuinely new engineering rather than a port
or a deletion. If it proves fiddly, the agreed fallback is spans without
propagation: the consumer opens a span per message and the `traceparent` field
is not added.
