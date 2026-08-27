# Best Practices Guide

This document explains **why this template exists**: it is a teaching
reference for the practices a production-grade Python microservice needs, not
just a FastAPI boilerplate. Each section below points at the real code that
demonstrates the practice, so you can read the pattern and then go see it
running.

If you only want to run the service, see [README.md](README.md). If you want
to understand the architecture layer by layer, see [CLAUDE.md](CLAUDE.md).
This document connects the two: it's the "why should I structure it this way"
companion to their "what" and "how".

---

## 1. Layered architecture (separation of concerns)

The codebase is split so that each layer has exactly one reason to change:

| Layer | Path | Responsibility |
|---|---|---|
| API | `api/` | HTTP request/response shape only (routers, Pydantic models) |
| Core | `core/services/` | Business sequencing — orchestrates data + messaging |
| Data | `db/` | Persistence: ORM models, repositories, migrations |
| Messaging | `messaging/` | Async pub/sub over Redis Streams |
| Config | `config/` | Settings, logging, tracing, request middleware |

**Why it matters:** routes never talk to the database or Redis directly —
they call a service (`core/services/order_service.py`), which calls a
repository (`db/repositories/order_repository.py`) and a producer
(`messaging/producer/redis_producer.py`). This makes each piece testable in
isolation (see §4) and means swapping Postgres or Redis for something else
touches one layer, not the whole codebase.

---

## 2. Structured logging with trace correlation

`config/logging.py` configures [structlog](https://www.structlog.org/) so
every log line is structured JSON (or console-pretty in dev) and automatically
carries the current OpenTelemetry `trace_id`/`span_id`. Get a logger with:

```python
from config.logging import get_logger
logger = get_logger(__name__)
logger.info("order_created", order_ref=order.order_ref)
```

**Why it matters:** structured, trace-correlated logs let you pivot from a
single failing request in your tracing backend straight to every log line
that request produced, across the API and the background consumer — instead
of grepping unstructured text.

`config/request_logger.py` adds a middleware that logs method, path, status,
and duration for every HTTP request automatically, so individual routes never
need to log their own request/response lifecycle.

---

## 3. Distributed tracing

`config/tracing.py` wires up OpenTelemetry with OTLP HTTP export and FastAPI
auto-instrumentation. Every request gets a trace spanning the API layer; the
logging layer (§2) stamps that same trace ID onto every log line emitted
during the request, so tracing and logging are two views of the same data,
not two disconnected systems.

---

## 4. Testing strategy: unit vs. integration

```bash
make test       # poetry run pytest -m "not integration" — no Docker required
make test-all   # poetry run pytest — includes testcontainers integration tests
```

- **Unit tests** (`tests/unit/`, run by `make test`) mock the database and
  Redis entirely and exercise service sequencing, dispatcher routing, and
  handler logic in isolation. They run in seconds, with no external
  dependencies, so they belong in every commit's feedback loop.
- **Integration tests** (`tests/integration/`, run only by `make test-all`)
  spin up real Postgres and Redis via
  [testcontainers](https://testcontainers-python.readthedocs.io/) and prove
  the full round trip: `POST /orders` → the consumer picks up the event →
  `GET /orders/{ref}` shows `confirmed`. This suite also includes a DLQ test
  and a migration `upgrade → downgrade → upgrade` test
  (`tests/integration/test_migrations.py`) — the only way to actually catch a
  revision whose `downgrade()` is broken or missing.

**Why it matters:** unit tests give fast, cheap confidence for every change;
integration tests are the only thing that proves the pieces actually work
together, including the parts (retries, DLQ, migrations) that are easy to get
subtly wrong.

**Why testing locally with Docker matters:** `make test` mocks Postgres and
Redis, so it can't catch a bad SQL query, a broken migration, a
serialization mismatch, or a Redis Streams consumer-group misconfiguration —
those only surface against the real thing. `make test-all` (testcontainers)
and `make up` (`docker-compose.yaml`) run the service against the same
Postgres and Redis versions used in CI/production, in a disposable,
containerized way — no "works on my machine because my local Postgres is a
different version" surprises, and no state leaking between runs since the
containers are torn down afterward. Treat `make test-all` (and a manual
`make up && make demo`) as required before opening a PR, not optional — the
unit suite is a fast first pass, not a substitute for proving the real
integration works.

---

## 5. Database access: layered, migration-only schema

- `db/models.py` — SQLAlchemy 2.0 async ORM models.
- `db/repositories/` — all query logic, kept out of services and routes.
- `db/postgres/session.py` — builds the async engine and session maker, and
  exposes the `get_db()` FastAPI dependency. It does **not** call
  `create_all()` — there is no such call anywhere in this codebase.
- `db/migrations/` — Alembic is the single source of truth for schema. Every
  revision must implement a working `downgrade()`, enforced by
  `tests/integration/test_migrations.py`.

**Why it matters:** a service that creates its own tables on startup has two
competing sources of truth (the ORM models and whatever the database actually
has) that silently drift apart. Routing every schema change through Alembic
means the checked-in revision history *is* the schema, reproducibly, in every
environment.

---

## 6. Messaging: async, at-least-once, with explicit failure handling

`messaging/consumer/redis_consumer.py` (`RedisConsumer`) uses Redis Streams
via `redis.asyncio.Redis` — never a synchronous client, which would block the
event loop that's also serving HTTP requests.

Failure handling is explicit, not an afterthought:

- **Retry with backoff:** a handler that raises is retried in-process up to
  `CONSUMER_MAX_RETRIES` times with exponential backoff
  (`CONSUMER_RETRY_BACKOFF_MS`).
- **Dead-lettering:** once retries are exhausted, the message goes to
  `DLQ_STREAM_NAME` (default `<STREAM_NAME>:dlq`) with its error, traceback,
  delivery count, and failure time, then gets acked so it leaves the pending
  list. A message that fails Pydantic validation skips retries and goes
  straight to the DLQ — a message that can't parse will never parse.
- **Crash recovery:** `XAUTOCLAIM` runs on startup and whenever a poll returns
  nothing, reclaiming messages orphaned by a killed consumer process. A
  reclaimed message already past `CONSUMER_MAX_RETRIES` deliveries is
  dead-lettered directly instead of retried again — the guard that stops a
  poison message from crashing the process forever.
- **Idempotent handlers:** Streams delivery is at-least-once, so redelivery
  is expected, not a bug. `messaging/consumer/handlers/order_created.py`
  achieves idempotency with a conditional
  `UPDATE ... WHERE status = 'pending'`; a redelivered message affects zero
  rows, logs "already processed," and acks normally.

**Why it matters:** these are the failure modes every real message consumer
hits in production — a bad message, a slow downstream, a process that gets
killed mid-batch. Handling them explicitly here means you inherit the pattern
instead of discovering it after an incident.

To add a new event type: add the payload model under
`messaging/models/events/`, add it to the `Literal` and `EventPayload` union
in `messaging/models/envelope.py`, write a handler under
`messaging/consumer/handlers/`, and register it in `HANDLERS` in
`messaging/consumer/dispatcher.py`.

---

## 7. Known, deliberate limitation: no transactional outbox

The order row commit and the `XADD` publish in
`core/services/order_service.py` are two separate operations, not one atomic
unit. If the commit succeeds but the publish fails, the API still returns
`201` with `status: "pending"` — the row is real, the event is not, and the
response says so truthfully instead of hiding it.

A production service closes this gap with a **transactional outbox**: write
the event to an outbox table in the same transaction as the domain row, then
have a separate relay process poll the outbox and publish, marking rows as
sent once delivered. This template omits it on purpose — it roughly doubles
the code for a reference slice whose job is to demonstrate the event flow
clearly. If you promote this template to a real service with strict
consistency requirements, this is the first gap to close.

---

## 8. Configuration and health checks

- `config/settings.py`: all environment configuration goes through Pydantic
  Settings, validated at startup rather than read ad hoc with `os.environ`
  scattered through the code. Access via the cached `get_settings()`.
- `api/routes/health.py`: the health check is not stubbed to always return
  200 — it actively probes Postgres (`SELECT 1`) and Redis (`PING`), each
  with a 2-second timeout, and returns `503` with per-dependency status if
  either is down.

**Why it matters:** a health check that doesn't check anything gives
orchestrators (Kubernetes, ECS, etc.) false confidence that a broken instance
is healthy. Validating configuration at startup, rather than failing deep
inside a request handler, turns misconfiguration into an immediate, loud
failure instead of a mysterious runtime bug.

---

## 9. Docs as part of the deliverable

- `README.md` — quickstart, the demo flow, the "make it yours" checklist.
- `CLAUDE.md` — architecture reference for AI-assisted development.
- `docs/` — specs and plans for larger changes.
- This file — the practices behind the structure, with pointers to the code.

**Why it matters:** a template that isn't explained is a template nobody
adopts correctly. Documentation here is written to be read end-to-end once,
then used as a reference — not a wall of comments duplicated across the code.

---

## How to use this template

1. Read `README.md`'s quickstart and run `make up && make demo` to see the
   whole flow (API write → event → consumer → confirmed) working.
2. Read this document to understand *why* each layer and pattern exists.
3. Follow the "Make it yours" checklist in `README.md` to replace the
   `orders` slice with your own domain, keeping every practice above intact.
