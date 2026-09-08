# S3 Object Storage in the Order Flow

**Date:** 2026-09-07
**Status:** Approved, ready for implementation planning

## Problem

The template demonstrates two infrastructure dependencies — PostgreSQL and
Redis Streams — and shows how they interact under at-least-once delivery.
Object storage is the third dependency most real services carry, and it is
absent from the application code. `docker-compose.yaml` already runs SeaweedFS
with an S3 endpoint on `:8333` and a `bucket-init` job that creates
`pmt-bucket`, but no Python code touches it: no dependency, no settings, no
layer.

A reader replacing the `orders` slice with their own domain therefore gets a
worked example of "database plus message broker" and nothing for "read config
from an object, write a derived object back." Those are the two S3 shapes most
services need, and both have failure and concurrency behaviour that is easy to
get wrong.

This design adds object storage to the existing order flow rather than beside
it, so the interaction between S3 and Redis is the thing on display.

## Approach

The `OrderCreated` consumer — not the HTTP request path — does the S3 work.
On each message it reads a price catalog object, prices the order, writes the
total to Postgres, and rebuilds the day's rollup object from SQL.

```
POST /orders ──┬─> orders row (pending, total_cents NULL)
               └─> outbox row                    [one transaction, unchanged]
                        │ relay
                        ▼
                   order.events (Redis Stream)
                        │
                 OrderCreated handler
                   ├─ 1. prices = PriceCatalog.get()   GET config/prices.json (TTL + If-None-Match)
                   ├─ 2. total_cents = unit_price_cents * quantity
                   ├─ 3. UPDATE orders SET total_cents, status='confirmed'
                   │       WHERE order_ref = ? AND status = 'pending'      [commit]
                   └─ 4. rebuild rollups/YYYY-MM-DD.json from SQL          PUT
                        ▼
                   GET /orders/{ref}   -> confirmed + total_cents
                   GET /rollups/{date} -> the object, streamed from S3
```

The read is object-as-config: cached in process, revalidated with an ETag. The
write is object-as-projection: derived from SQL, never merged in place.

### Decisions and their reasons

**S3 lives in the consumer, not the request path.** `POST /orders` keeps its
current shape — one transaction, two rows, no network call to anything but
Postgres. A 201 continues to mean "committed and will be published," and no S3
outage can slow or fail an order submission. It also puts the S3 calls
somewhere that already has retry, backoff and dead-lettering, so failure
handling is composition rather than new machinery.

**Postgres is authoritative; the rollup object is a projection.** The handler
does not read the rollup, merge one order into it, and write it back. It
rebuilds the whole day from a `SELECT` and overwrites the object. Read-modify-
write on a shared object across replicas loses updates unless every writer
uses a conditional PUT, and the correctness of that scheme then depends on the
S3 implementation honouring `If-Match`. Rebuilding from SQL needs no
coordination: concurrent writers race, the last one wins, and the winner is
correct because every writer computed from the same authoritative source. The
object can be briefly stale; it can never be wrong.

**The rollup is rebuilt unconditionally, including on redelivery.** Step 3 is
a conditional `UPDATE ... WHERE status = 'pending'`, so a redelivered message
affects zero rows. It is tempting to skip step 4 when that happens. That
breaks recovery: if the PUT fails after a successful commit, the message
retries, the UPDATE is now a no-op, and the rollup would never be written at
all. Running step 4 on every delivery makes a retry converge the object, which
is the same reason the outbox relay republishes rather than tracking what it
believes it already sent.

**Money is integers, everywhere.** `prices.json` denominates in
`unit_price_cents`, Postgres stores `total_cents BIGINT`, and the API returns
`total_cents`. There is one representation from the object through the
database to the JSON response, so there is no conversion boundary and no
rounding step. `BIGINT` rather than `INT` because a 32-bit column caps at about
$21M. A non-integer `unit_price_cents` in the catalog is a validation failure,
not a truncation.

**Failures are classified, mirroring the outbox relay.** `storage/s3/errors.py`
answers the same question `messaging/outbox/backoff.py` answers for Redis:
might this succeed later? Transport failures and 5xx say yes; a missing or
malformed catalog says no. The trap is different from the Redis one: `boto3`
raises `ClientError` for both a 503 and a `NoSuchKey`, so the classifier
branches on the response code, not the exception class.

**aioboto3, not boto3 in a thread.** The consumer shares its event loop with
the HTTP server. A synchronous client here would block it, which is the same
reason `RedisConsumer` uses `redis.asyncio`.

## Data model

### `orders.total_cents`

Migration `0004_add_order_total_cents`:

```
total_cents BIGINT NULL
```

Nullable: a `pending` order has no price yet, and existing rows have none.
`downgrade()` drops the column — `tests/integration/test_migrations.py`
round-trips every revision.

### `config/prices.json` (S3)

```json
{
  "currency": "USD",
  "items": {
    "widget": {"unit_price_cents": 1999},
    "gadget": {"unit_price_cents": 4550}
  }
}
```

Validated by a Pydantic model with `unit_price_cents: int = Field(ge=0)`. A
float value fails validation rather than truncating. Seeded by `bucket-init`
so `make up` yields a working demo instead of an immediate dead-letter.

### `rollups/YYYY-MM-DD.json` (S3)

```json
{
  "date": "2026-09-07",
  "currency": "USD",
  "total_cents": 5997,
  "order_count": 1,
  "orders": [
    {"order_ref": "abc", "item": "widget", "quantity": 3,
     "unit_price_cents": 1999, "total_cents": 5997}
  ]
}
```

A rollup covers the orders **created** on that UTC date — `created_at`, not
`confirmed_at` — so an order never moves between rollups when confirmation
lands after midnight. The document contains only rows already `confirmed`
(a `pending` order has no price to report), ordered by `confirmed_at`.

## Components

### `storage/object_store.py` — `ObjectStore`

A Protocol, so the handler and services depend on an interface rather than on
`aioboto3`:

```python
class ObjectData(NamedTuple):
    body: bytes
    etag: str


class NotModified(Enum):
    """Single-member enum so the sentinel has a type a checker can narrow."""
    token = auto()


NOT_MODIFIED = NotModified.token


class ObjectStore(Protocol):
    async def get(self, key: str) -> ObjectData | None: ...
    async def get_if_none_match(
        self, key: str, etag: str
    ) -> ObjectData | NotModified | None: ...
    async def put(self, key: str, body: bytes, content_type: str) -> None: ...
    async def head_bucket(self) -> None: ...
```

`get` returns `None` for a missing key; callers decide whether absence is an
error. `get_if_none_match` returns `NOT_MODIFIED` on a 304 and `None` when the
key is gone.

### `storage/s3/client.py` — `get_object_store()`

Builds an `aioboto3` session from settings and holds one long-lived client as
a module singleton, entered in the application lifespan and closed on
shutdown. Same shape as `get_producer()` and `get_relay()`, so a reader meets
a pattern they have already seen twice. Connect and read timeouts come from
settings; botocore's own retries are disabled so that retry policy lives in
one place — the consumer.

### `storage/s3/errors.py` — `is_retryable`

```python
RETRYABLE_CODES = {"500", "502", "503", "504", "SlowDown", "RequestTimeout",
                   "InternalError", "ServiceUnavailable"}
```

`is_retryable(exc)` is true for `EndpointConnectionError`,
`ConnectTimeoutError`, `ReadTimeoutError`, and for a `ClientError` whose
response code is in `RETRYABLE_CODES`. Everything else — `NoSuchKey`,
`NoSuchBucket`, `AccessDenied`, a `ValidationError` from parsing the catalog,
an unknown item — is permanent.

The module also defines `PermanentHandlerError`, raised by callers for the
permanent cases.

### `storage/fake.py` — `FakeObjectStore`

An in-memory `ObjectStore`: a dict of key to `(bytes, etag)`, with the etag
recomputed on put. Supports injecting an exception on the next call, so unit
tests can drive both branches of the classifier without Docker.

### `core/services/pricing.py` — `PriceCatalog`

Holds the parsed catalog, its ETag, and the time it was loaded.

- Within `PRICES_CACHE_TTL_S` of the last load, `get()` returns the cached
  catalog with no network call.
- After the TTL, `get()` issues a conditional GET with `If-None-Match`. A 304
  refreshes the timestamp and keeps the cached object. A 200 parses and
  replaces it.
- An `asyncio.Lock` guards the refresh so a batch of concurrent messages
  causes one GET, not `CONSUMER_BATCH_SIZE` of them.
- A missing key, a body that is not JSON, or a body failing the Pydantic model
  raises `PermanentHandlerError`. Transport failures propagate for the
  classifier to judge.
- On a failed revalidation the cached catalog is **not** discarded; the error
  propagates and the message retries. A prices file that becomes briefly
  unreachable does not invalidate what the process already holds.

### `core/services/rollup_service.py` — `RollupService`

`rebuild_for_date(day)`: selects the confirmed orders created on `day`, serializes the
document above, and PUTs it to `f"{S3_ROLLUP_PREFIX}{day}.json"` with
`content_type="application/json"`. No read of the existing object.

### `db/repositories/order_repository.py`

- `confirm(order_ref, total_cents)` — the existing conditional `UPDATE` gains
  the `total_cents` assignment. Return value stays "did this affect a row."
- `list_confirmed_created_on(day)` — orders with `status='confirmed'` whose
  `created_at` falls on `day` in UTC, ordered by `confirmed_at`. Served by the
  existing `ix_orders_status_created` index. Named for both predicates because
  the two timestamps can disagree across a midnight boundary.

### `messaging/consumer/handlers/order_created.py`

Sequences the four steps. It contains no pricing arithmetic and no
serialization: those live in the two services. An unknown item raises
`PermanentHandlerError`.

### `messaging/consumer/redis_consumer.py`

One change. The retry loop in `_handle` currently catches bare `Exception` and
spends `CONSUMER_MAX_RETRIES` attempts on every failure. It gains a branch
that dead-letters `PermanentHandlerError` on sight with reason
`permanent_handler_error`, alongside the `ValidationError` branch that already
behaves this way. Without this, the classifier in `storage/s3/errors.py` has
no effect on runtime behaviour.

### `api/routes/orders.py` and `api/routes/models.py`

`OrderResponse` gains `total_cents: int | None`. No route logic changes.

### `api/routes/rollups.py` (new)

`GET /rollups/{date}` streams the object for that date. 404 when the key is
absent, 503 when the store is unreachable. Registered in `main.py` beside the
existing routers.

### `api/routes/health.py`

A third probe: `head_bucket()` under the same 2-second timeout as the Postgres
and Redis probes, reported per-dependency, 503 on failure.

### `main.py`

The lifespan enters the S3 client before starting the consumer and relay, and
closes it after they stop.

### `pyproject.toml`

`aioboto3` as a runtime dependency (it pulls `aiobotocore` and `botocore`).
No new dev dependency: the SeaweedFS fixture uses the generic
`testcontainers.core.container.DockerContainer` already available through the
installed `testcontainers` package.

### `config/settings.py`

```
S3_ENDPOINT_URL: str = "http://localhost:8333"
S3_REGION: str = "us-east-1"
S3_BUCKET: str = "pmt-bucket"
S3_ACCESS_KEY_ID: str = "dev"
S3_SECRET_ACCESS_KEY: str = "dev"
S3_PRICES_KEY: str = "config/prices.json"
S3_ROLLUP_PREFIX: str = "rollups/"
S3_CONNECT_TIMEOUT_S: int = 2
S3_READ_TIMEOUT_S: int = 5
PRICES_CACHE_TTL_S: int = 60
```

Mirrored into `.env.example` and the compose `api` service, which also gains
`depends_on: seaweedfs (condition: service_healthy)`.

## Testing

### Unit — `make test`, no Docker

All driven by `FakeObjectStore`.

- `PriceCatalog`: first load fetches; a call within the TTL does not; after the
  TTL a 304 keeps the cached catalog and resets the timer; a 200 replaces it;
  concurrent callers across an expired TTL produce exactly one GET; a failed
  revalidation propagates and leaves the cache intact.
- Catalog validation: missing key, non-JSON body, and a float
  `unit_price_cents` each raise `PermanentHandlerError`.
- `is_retryable`: a 503 `ClientError` is retryable, a `NoSuchKey` `ClientError`
  is not, connection and timeout errors are, a `ValidationError` is not.
- Handler: happy path writes the total and the rollup; an unknown item raises
  `PermanentHandlerError`; a retryable store failure propagates; a redelivery
  affects zero rows and still rebuilds the rollup.
- `RollupService`: the serialized document matches the shape above, including
  `total_cents` as the sum of its rows.
- Consumer: a handler raising `PermanentHandlerError` is dead-lettered on the
  first attempt with no retries; a generic exception still retries.

### Integration — `make test-all`

Backed by a `DockerContainer("chrislusf/seaweedfs:4.44")` fixture configured
the way `docker-compose.yaml` configures it — the shipped server, not a
substitute, so a behaviour SeaweedFS implements differently from another S3
cannot pass unnoticed.

- Round trip: seed `config/prices.json`, POST an order, wait for confirmation,
  assert `total_cents` on the row and a rollup object whose contents match.
- Conditional GET against the real server: a second load after the TTL returns
  304 and no re-parse.
- `GET /rollups/{date}`: 200 with the object, 404 for a date with no object.
- `/health`: 503 when the bucket is unreachable.

## Documentation

- `CLAUDE.md`: a "Storage" section covering the layer, the projection rule, the
  rebuild-on-redelivery rule, and the integer-money rule; the Consumer section
  notes the new permanent-error path.
- `README.md`: S3 in the architecture description, the new settings, and
  `make demo-s3`.
- `Makefile`: `demo-s3` — seed prices, POST an order, poll to confirmation, cat
  the rollup back out of the bucket.

## Out of scope

- Conditional writes (`If-Match`) and any read-modify-write on a shared object.
  The projection design removes the need, and SeaweedFS's support for the
  header is unverified.
- Multipart uploads, presigned URLs, object versioning, lifecycle rules.
- Serving the rollup from a cache or CDN.
- Backfilling `total_cents` for orders confirmed before this change; the column
  stays NULL for them.
