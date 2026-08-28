# Best Practices Guide

This document explains **why this template exists**: it is a teaching
reference for the practices a production-grade Python message-processing
service needs, not just a Redis Streams boilerplate. Each section below points
at the real code that demonstrates the practice, so you can read the pattern
and then go see it running.

If you only want to run the service, see [README.md](README.md). If you want
to understand the architecture layer by layer, see [CLAUDE.md](CLAUDE.md).
This document connects the two: it's the "why should I structure it this way"
companion to their "what" and "how".

---

## 1. Layered architecture (separation of concerns)

The codebase is split so that each layer has exactly one reason to change:

| Layer | Path | Responsibility |
|---|---|---|
| Messaging | `messaging/` | Async pub/sub over Redis Streams: envelopes, producer, consumer, handlers |
| Handler state | `messaging/state.py` | The handler-side Redis client, separate from the consumer's |
| Health | `health/` | A probe endpoint an orchestrator can call |
| Entry points | `main.py`, `cli.py` | The worker process, and the one-shot publisher |
| Config | `config/` | Settings, logging, tracing |

**Why it matters:** a handler never reaches into the consumer's connection,
and the consumer never knows what a handler does with a payload. The
dispatcher (`messaging/consumer/dispatcher.py`) is the only thing that knows
which handler serves which event type, so adding an event type is a
registry edit rather than a change to the read loop. Each piece is testable
in isolation (see §4).

Note what is deliberately *absent*: there is no API layer, no service layer,
and no ORM. A worker that consumes events needs none of them, and every layer
you keep is a layer you maintain.

---

## 2. Structured logging with trace correlation

`config/logging.py` configures [structlog](https://www.structlog.org/) so
every log line is structured JSON (or console-pretty in dev) and automatically
carries the current OpenTelemetry `trace_id`/`span_id`. Get a logger with:

```python
from config.logging import get_logger
logger = get_logger(__name__)
logger.info("Order confirmed", order_ref=payload.order_ref)
```

**Why it matters:** structured, trace-correlated logs let you pivot from a
single failing event in your tracing backend straight to every log line that
event produced, across the publisher and the consumer — instead of grepping
unstructured text. Because trace context crosses the stream (§3), the
`trace_id` on the CLI's "Event published" line is the same `trace_id` on the
worker's "Order confirmed" line, in a different process.

---

## 3. Distributed tracing that survives the hop

`config/tracing.py` wires up OpenTelemetry with OTLP HTTP export. There is no
auto-instrumentation to lean on here — a worker has no incoming HTTP request
to hang a trace off — so spans are created explicitly:

- The producer opens `publish <EventType>` and **injects** the current W3C
  trace context into `EventEnvelope.traceparent` before the `XADD`.
- The consumer **extracts** that context and opens `consume <EventType>` as
  its child, with `start_as_current_span`.

That gives one unbroken trace across both process hops:

```
[cli.publish]                              (cli process, root)
  └── [publish OrderCreated]
        └── [consume OrderCreated]         (worker)
              └── [publish OrderConfirmed]
                    └── [consume OrderConfirmed]
```

**Why it matters, and the one rule to preserve:** the consumer's span must be
made *current*, not held as a detached span. The producer reads ambient
context and takes no span argument, which is exactly what lets the same
producer serve the CLI and a handler with no argument threading — but it also
means a detached consumer span silently orphans everything the handler
publishes. A detached span logs identically, exports identically, and looks
fine in every test that only asserts spans exist. That is why
`tests/integration/test_tracing.py` asserts on parent span *ids*.

An envelope with no `traceparent` simply starts a new trace, so a publisher
outside a trace is not an error.

---

## 4. Testing strategy: unit vs. integration

```bash
make test       # poetry run pytest -m "not integration" — no Docker required
make test-all   # poetry run pytest — includes testcontainers integration tests
```

- **Unit tests** (`tests/unit/`, run by `make test`) need nothing external and
  exercise envelope validation, dispatcher routing, money formatting, the
  state client's lifecycle, and the worker's startup/shutdown sequencing with
  a fake consumer. They run in under a second, so they belong in every
  commit's feedback loop.
- **Integration tests** (`tests/integration/`, run only by `make test-all`)
  spin up a real Redis via
  [testcontainers](https://testcontainers-python.readthedocs.io/) and prove
  the full round trip: the CLI publishes `OrderCreated` twice → the consumer
  confirms once, computes the total, publishes exactly one `OrderConfirmed` →
  the consumer handles that too. This suite also covers the DLQ, the health
  probe's 503 path, and end-to-end trace parentage.

**Why it matters:** unit tests give fast, cheap confidence for every change;
integration tests are the only thing that proves the pieces actually work
together, including the parts (retries, DLQ, idempotency, trace propagation)
that are easy to get subtly wrong.

**Why testing locally with Docker matters:** `make test` never touches Redis,
so it can't catch a consumer-group misconfiguration, a serialization
mismatch, an `HSETNX` guard that doesn't guard, or a span that got detached.
Those only surface against the real thing. `make test-all` (testcontainers)
and `make up` (`docker-compose.yaml`) run the service against the same Redis
version used in CI/production, in a disposable, containerized way — no "works
on my machine" surprises, and no state leaking between runs since the
containers are torn down afterward. Treat `make test-all` (and a manual
`make up && make demo`) as required before opening a PR, not optional — the
unit suite is a fast first pass, not a substitute for proving the real
integration works.

---

## 5. Money as integer minor units

Amounts are integers everywhere: `450` means `4.50`, and the field name
carries the unit (`unit_price_cents`, `total_cents`). Never `float`, never
`Decimal` on the wire. `money.py` holds the single function that turns cents
into a human-readable string, and it is called only at output edges — log
lines and CLI output.

**Why it matters:** floats cannot represent money exactly, and a `Decimal`
has to be serialized as *something* the moment it crosses a stream — usually
a string, whose precision and format then become an undocumented contract
between services. An integer serializes as itself, is exact under addition
and multiplication, and lands in a consumer written in any other language
with no precision conversation. Keeping formatting at the edges means the
wire format never drifts to follow presentation: `4.5` submitted as a unit
price is rejected at validation rather than silently truncated somewhere
downstream.

---

## 6. Messaging: async, at-least-once, with explicit failure handling

`messaging/consumer/redis_consumer.py` (`RedisConsumer`) uses Redis Streams
via `redis.asyncio.Redis` — never a synchronous client, which would block the
event loop the health server also runs on.

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
  achieves idempotency with `HSETNX order:<ref> status confirmed`: exactly one
  delivery per reference writes the field and takes the work path; a
  redelivered message sees `0`, logs "already processed," and acks normally.

**Why it matters:** these are the failure modes every real message consumer
hits in production — a bad message, a slow downstream, a process that gets
killed mid-batch. Handling them explicitly here means you inherit the pattern
instead of discovering it after an incident.

### Consume → compute → publish

The `OrderCreated` handler is not a leaf: it computes a total and publishes
`OrderConfirmed`, which the same worker then consumes. That middle hop is the
point of the template, because it is the shape most real services take, and it
carries two rules worth internalizing:

- **Put the idempotency guard's early return *before* the publish.** Correct
  local state is only half of idempotency. If a redelivery returns early only
  after publishing, one duplicate inbound event becomes a duplicate outbound
  event, and it amplifies through every consumer downstream.
- **Never publish upstream from a handler.** Both sample events share one
  stream, so a handler that republishes what it consumes is an infinite loop
  that looks exactly like a busy worker.

Handlers also take their own Redis client (`messaging/state.py`) rather than
borrowing the consumer's. There is no request scope here to inherit a
connection from, and keeping them apart means a handler cannot accidentally
issue a blocking command on the connection the read loop depends on.

To add a new event type: add the payload model under
`messaging/models/events/`, add it to the `Literal` and `EventPayload` union
in `messaging/models/envelope.py`, write a handler under
`messaging/consumer/handlers/`, and register it in `HANDLERS` in
`messaging/consumer/dispatcher.py`. `OrderConfirmed` is a worked example of
exactly that four-file edit.

---

## 7. Known, deliberate limitation: no transactional outbox

The handler's state write and the `XADD` publish in
`messaging/consumer/handlers/order_created.py` are two separate operations,
not one atomic unit. If the state write succeeds and the publish fails, the
handler raises and the message is retried — but the retry finds the `HSETNX`
guard already set and takes the early return, so `OrderConfirmed` is never
published. The state is right and the downstream event is lost.

A production service closes this gap with a **transactional outbox**: write
the outgoing event in the same atomic write as the state, then have a separate
relay drain the outbox to the stream, marking entries as sent once delivered.
This template omits it on purpose — it roughly doubles the moving parts for a
reference whose job is to make the mechanism legible. The failure is named in
the handler's docstring so nobody meets it by surprise. If you promote this
template to a real service with strict consistency requirements, this is the
first gap to close.

(The same "simpler beats stronger, but say so out loud" judgment shows up
one level down: `HSETNX` and `EXPIRE` are two round trips, so a crash between
them leaves a state key with no TTL. A Lua script would make it atomic. Don't
copy that shape into a place where the TTL is load-bearing.)

---

## 8. Configuration and health checks

- `config/settings.py`: all environment configuration goes through Pydantic
  Settings, validated at startup rather than read ad hoc with `os.environ`
  scattered through the code. Access via the cached `get_settings()`. Every
  field has a default, so the template runs with no configuration at all.
- `health/server.py`: the health check is not stubbed to always return 200 —
  it actively probes Redis (`PING`) with a 2-second timeout and returns `503`
  with `{"status": "degraded", "redis": "down"}` if the probe fails. It is
  about forty lines of `asyncio.start_server`, not an ASGI framework: one
  route, no keep-alive, no routing table.

**Why it matters:** a health check that doesn't check anything gives
orchestrators (Kubernetes, ECS, etc.) false confidence that a broken instance
is healthy — and for a worker, "broken" usually means "cannot reach the stream
it exists to consume," which is invisible from the outside. `main.py` closes
the same loop from the other side: if the consumer task dies, the worker stops
rather than sitting there answering probes with a dead read loop. Validating
configuration at startup, rather than failing deep inside a handler, turns
misconfiguration into an immediate, loud failure instead of a mysterious
runtime bug.

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
   whole flow (CLI publish → consume → compute → publish → consume) working.
2. Read this document to understand *why* each layer and pattern exists.
3. Follow the "Make it yours" checklist in `README.md` to replace the
   `orders` events with your own domain, keeping every practice above intact.
