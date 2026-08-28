# Python Microservice Template — Redis Streams

An async worker template with a working reference pipeline: a CLI publishes an
event, a consumer picks it up, computes a result, and publishes a follow-on
event — all under one distributed trace. Read it end to end, then replace the
sample events with your own.

There is no HTTP API and no database. This is a message-processing service.

---

## Quickstart

```bash
make up      # docker compose up --build -d; starts Redis and the worker
make demo    # publish one order twice; watch it confirm exactly once
```

Expected output:

```
--- publishing OrderCreated x2 (same ref)
published 2 x OrderCreated ref=demo-1 qty=3 unit=4.50 expected_total=13.50
  1735300000000-0
  1735300000000-1
--- waiting for the consumer...
--- state in Redis
status
confirmed
item
widget
quantity
3
total_cents
1350
--- worker log
... "Order confirmed" total=13.50
... "Order already processed; nothing to do"
... "Order confirmation received" total=13.50
```

Two events in, one confirmation out. The second delivery is a logged no-op —
that is the idempotency guard doing its job, not a bug.

---

## What the demo does

```
python -m cli publish --ref demo-1 --quantity 3 --unit-price-cents 450 --count 2
  └→ XADD order.events "OrderCreated"   x2

[worker] XREADGROUP → OrderCreated (1st)
  └→ HSETNX order:demo-1 status confirmed → 1
  └→ total_cents = 3 * 450 = 1350        [the processing step]
  └→ XADD order.events "OrderConfirmed" {total_cents: 1350}
  └→ XACK

[worker] XREADGROUP → OrderCreated (2nd)
  └→ HSETNX → 0 → "already processed", publishes nothing
  └→ XACK

[worker] XREADGROUP → OrderConfirmed
  └→ log; XACK
```

The consumer is both a consumer and a producer. That middle hop is the point of
the template: consume → compute → publish is the shape most real services take.

---

## One trace across both hops

Trace context rides in a `traceparent` field on the envelope. The producer
injects whatever span is current; the consumer extracts it and makes its
message span current, so anything a handler publishes is automatically a child
of the message being handled:

```
[cli.publish]                              (cli process, root)
  └── [publish OrderCreated]
        └── [consume OrderCreated]         (worker)
              └── [publish OrderConfirmed]
                    └── [consume OrderConfirmed]
```

Point `OTEL_EXPORTER_OTLP_ENDPOINT` at a collector to see it, or set
`OTEL_EXPORTER_OTLP_ENDPOINT_ENABLE_FALLBACK=True` to dump spans to the console.

The single rule to preserve if you touch this: the consumer's span must be
made **current** (`start_as_current_span`), not held detached. A detached span
looks identical in the logs and silently orphans every downstream hop.

---

## Health

The worker serves one endpoint, on `HEALTH_PORT` (8000 in compose):

```bash
make health     # 200 {"status":"ok","redis":"ok"} or 503 {"status":"degraded",...}
```

It is about forty lines of `asyncio.start_server` in `health/server.py`, not a
web framework. There is no `/info` — `SERVICE_NAME`, `SERVICE_VERSION` and
`ENVIRONMENT` are stamped on every exported span as resource attributes, so
the collector already has them.

---

## Money

Amounts are integer minor units everywhere: `450` means `4.50`, and the field
name carries the unit (`unit_price_cents`, `total_cents`). Never floats.
Formatting to a human-readable string happens only at output edges, via
`format_cents()` in `money.py` — payload models carry ints, so the wire format
never depends on presentation.

---

## What this template does not solve

The handler writes its state and then publishes. Those two steps are not
atomic: if the publish fails after the state write, the retry sees the
idempotency guard and skips the publish, so the downstream event is lost. This
is the write-then-publish problem, and the real answer is a transactional
outbox — persist the outgoing event in the same write as the state, and let a
separate relay drain it to the stream.

That is deliberately not implemented here. It roughly doubles the moving parts,
and a template's job is to make the mechanism legible. The failure is named in
`messaging/consumer/handlers/order_created.py` so nobody meets it by surprise.

---

## Make it yours

The sample events exist to be replaced. Edit these four places, in order:

1. `messaging/models/events/` — your payloads, in place of `OrderCreatedEvent`
   and `OrderConfirmedEvent`.
2. `messaging/models/envelope.py` — add your event types to the `Literal` and
   to the `EventPayload` union.
3. `messaging/consumer/handlers/` — your handlers.
4. `messaging/consumer/dispatcher.py` — register them in `HANDLERS`.

`OrderConfirmed` is a worked example of exactly that four-file edit; follow it.

Two rules to keep when you do:

- **Handlers must be idempotent.** Redis Streams delivers at least once.
  Guard with a conditional write, and put the early return *before* any
  publish — otherwise one redelivery amplifies through every downstream
  consumer.
- **Do not publish upstream from a handler.** Both sample events share one
  stream; a handler that republishes what it consumes is an infinite loop that
  looks like a busy worker.

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

---

## Testing

```bash
make test       # poetry run pytest -m "not integration" — no Docker required
make test-all   # poetry run pytest — includes testcontainers-backed integration tests
```

`make test` runs the unit suite (dispatcher routing, envelope validation,
money formatting, worker lifecycle) with nothing external required, so it works
without Docker. `make test-all` additionally spins up a real Redis via
testcontainers and runs the handler, health, tracing, CLI and round-trip
tests — this is the suite that actually proves the template works.
