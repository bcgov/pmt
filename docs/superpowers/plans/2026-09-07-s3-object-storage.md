# S3 Object Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add object storage to the existing order flow — the `OrderCreated`
consumer reads a price catalog from S3, writes the total to Postgres, and
rebuilds a daily rollup object back into S3.

**Architecture:** A new `storage/` layer sits beside `db/` and `messaging/`,
exposing an `ObjectStore` Protocol backed by `aioboto3` in production and an
in-memory fake in unit tests. Two services in `core/services/` hold the domain
logic: `PriceCatalog` (TTL + ETag-revalidated read) and `RollupService`
(projection rebuilt from SQL and overwritten, never merged). The HTTP write
path is untouched; all S3 work happens in the consumer, where retry, backoff
and dead-lettering already exist.

**Tech Stack:** Python 3.14, FastAPI, SQLAlchemy 2.0 async, Alembic,
`redis.asyncio`, `aioboto3`, SeaweedFS (S3 API), pytest + testcontainers.

**Spec:** `docs/superpowers/specs/2026-09-07-s3-object-storage-design.md`

## Global Constraints

- Money is integers everywhere: `unit_price_cents` in the catalog,
  `total_cents BIGINT` in Postgres, `total_cents` in the API response. No
  `Decimal`, no float, no conversion at any boundary.
- The S3 client must be async (`aioboto3`). A blocking client would stall the
  event loop that also serves HTTP.
- Line length 88 (Black). Ruff selects `E, F, W, B, I`; `E501` and `B008` are
  ignored.
- Unit tests run with no Docker. Anything needing a container lives in
  `tests/integration/` and carries `pytestmark = pytest.mark.integration`.
- Every Alembic revision implements `downgrade()` —
  `tests/integration/test_migrations.py` round-trips them.
- Repositories flush but never commit; transaction boundaries belong to the
  caller.
- The rollup object is never read-modify-written. It is rebuilt from SQL and
  overwritten.
- Run `make fmt && make lint` before each commit.

## File Structure

**Created:**

| File | Responsibility |
|---|---|
| `storage/__init__.py` | Re-exports `ObjectStore`, `ObjectData`, `NOT_MODIFIED`, `get_object_store`, `close_object_store` |
| `storage/object_store.py` | The `ObjectStore` Protocol, `ObjectData`, the `NOT_MODIFIED` sentinel |
| `storage/errors.py` | `PermanentHandlerError`, `is_retryable(exc)` |
| `storage/fake.py` | `FakeObjectStore` — in-memory implementation for unit tests |
| `storage/s3/__init__.py` | Package marker |
| `storage/s3/client.py` | `S3ObjectStore`, `get_object_store()`, `close_object_store()` |
| `core/services/pricing.py` | `PriceCatalog`, `PriceCatalogDocument`, `get_price_catalog()`, `reset_price_catalog()` |
| `core/services/rollup_service.py` | `RollupService.rebuild_for_date()` |
| `api/routes/rollups.py` | `GET /rollups/{date}` |
| `db/migrations/versions/0004_add_order_total_cents.py` | `orders.total_cents BIGINT NULL` |
| `tests/unit/test_object_store_fake.py` | `FakeObjectStore` behaviour |
| `tests/unit/test_storage_errors.py` | The retryable/permanent classifier |
| `tests/unit/test_pricing.py` | Cache, revalidation, validation failures |
| `tests/unit/test_rollup_service.py` | Rollup document shape |
| `tests/unit/test_order_created_handler_s3.py` | Handler sequencing against the fake |
| `tests/integration/test_s3_object_store.py` | `S3ObjectStore` against SeaweedFS |
| `tests/integration/test_rollup_routes.py` | `GET /rollups/{date}` |
| `tests/integration/test_order_s3_roundtrip.py` | POST → relay → consumer → rollup object |

**Modified:**

| File | Change |
|---|---|
| `pyproject.toml` | `aioboto3` runtime dependency |
| `config/settings.py` | Ten `S3_*` / `PRICES_CACHE_TTL_S` settings |
| `.env.example` | The same settings |
| `docker-compose.yaml` | `S3_*` env on `api`, `depends_on: seaweedfs`, `bucket-init` seeds `config/prices.json` |
| `db/models.py` | `Order.total_cents` |
| `db/repositories/order_repository.py` | `confirm(order_ref, total_cents)`, `list_confirmed_created_on(day)` |
| `messaging/consumer/redis_consumer.py:150-169` | Dead-letter `PermanentHandlerError` without retries |
| `messaging/consumer/handlers/order_created.py` | Price, confirm with total, rebuild rollup |
| `api/routes/models.py` | `OrderResponse.total_cents` |
| `api/routes/health.py` | S3 `head_bucket` probe |
| `main.py` | Enter/close the S3 client in the lifespan; include the rollups router |
| `tests/conftest.py` | `seaweedfs_url` session fixture, `s3_settings`, `object_store` |
| `Makefile` | `demo-s3` target |
| `README.md`, `CLAUDE.md` | Storage documentation |

---

### Task 1: Dependency and settings

**Files:**
- Modify: `pyproject.toml`
- Modify: `config/settings.py`
- Modify: `.env.example`
- Test: `tests/unit/test_settings_s3.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: `Settings` attributes `S3_ENDPOINT_URL: str`, `S3_REGION: str`,
  `S3_BUCKET: str`, `S3_ACCESS_KEY_ID: str`, `S3_SECRET_ACCESS_KEY: str`,
  `S3_PRICES_KEY: str`, `S3_ROLLUP_PREFIX: str`, `S3_CONNECT_TIMEOUT_S: int`,
  `S3_READ_TIMEOUT_S: int`, `PRICES_CACHE_TTL_S: int`, and the property
  `rollup_key(day: date) -> str`.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_settings_s3.py`:

```python
from datetime import date


def test_s3_settings_have_local_defaults():
    from config.settings import Settings

    settings = Settings(DATABASE_URL="postgresql+asyncpg://x:y@localhost/z")

    assert settings.S3_ENDPOINT_URL == "http://localhost:8333"
    assert settings.S3_BUCKET == "pmt-bucket"
    assert settings.S3_PRICES_KEY == "config/prices.json"
    assert settings.S3_ROLLUP_PREFIX == "rollups/"
    assert settings.PRICES_CACHE_TTL_S == 60


def test_rollup_key_joins_prefix_and_iso_date():
    from config.settings import Settings

    settings = Settings(DATABASE_URL="postgresql+asyncpg://x:y@localhost/z")

    assert settings.rollup_key(date(2026, 9, 7)) == "rollups/2026-09-07.json"


def test_rollup_key_respects_a_custom_prefix():
    from config.settings import Settings

    settings = Settings(
        DATABASE_URL="postgresql+asyncpg://x:y@localhost/z",
        S3_ROLLUP_PREFIX="daily/",
    )

    assert settings.rollup_key(date(2026, 9, 7)) == "daily/2026-09-07.json"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/unit/test_settings_s3.py -v`
Expected: FAIL — `AttributeError: 'Settings' object has no attribute 'S3_ENDPOINT_URL'`

- [ ] **Step 3: Add the dependency**

In `pyproject.toml`, under `[tool.poetry.dependencies]`, after the `# --- Redis ---` block:

```toml
# --- Object storage (S3 API) ---
aioboto3 = "^15.4.0"
```

Run: `poetry lock && poetry install`

- [ ] **Step 4: Add the settings**

In `config/settings.py`, add `from datetime import date` at the top of the
imports, then insert this block after the `# Outbox relay` block:

```python
    # -------------------------
    # Object storage (S3 API)
    # -------------------------
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

And add this property next to the existing `dlq_stream` property:

```python
    def rollup_key(self, day: date) -> str:
        """Object key for one day's rollup: '<prefix><YYYY-MM-DD>.json'."""
        return f"{self.S3_ROLLUP_PREFIX}{day.isoformat()}.json"
```

- [ ] **Step 5: Run test to verify it passes**

Run: `poetry run pytest tests/unit/test_settings_s3.py -v`
Expected: PASS (3 passed)

- [ ] **Step 6: Mirror into `.env.example`**

Append to `.env.example`, before the `# Logging` block:

```
# Object storage (S3 API)
S3_ENDPOINT_URL=http://localhost:8333
S3_REGION=us-east-1
S3_BUCKET=pmt-bucket
S3_ACCESS_KEY_ID=dev
S3_SECRET_ACCESS_KEY=dev
S3_PRICES_KEY=config/prices.json
S3_ROLLUP_PREFIX=rollups/
S3_CONNECT_TIMEOUT_S=2
S3_READ_TIMEOUT_S=5
PRICES_CACHE_TTL_S=60
```

- [ ] **Step 7: Commit**

```bash
make fmt && make lint
git add pyproject.toml poetry.lock config/settings.py .env.example tests/unit/test_settings_s3.py
git commit -m "feat: add S3 settings and the aioboto3 dependency"
```

---

### Task 2: The ObjectStore interface and its fake

**Files:**
- Create: `storage/__init__.py`
- Create: `storage/object_store.py`
- Create: `storage/fake.py`
- Test: `tests/unit/test_object_store_fake.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `ObjectData(body: bytes, etag: str)` NamedTuple;
  `NotModified` enum with `NOT_MODIFIED` sentinel; `ObjectStore` Protocol with
  `get(key) -> ObjectData | None`,
  `get_if_none_match(key, etag) -> ObjectData | NotModified | None`,
  `put(key, body, content_type) -> None`, `head_bucket() -> None`;
  `FakeObjectStore(objects: dict[str, bytes] | None = None)` with attributes
  `get_calls: int`, `puts: dict[str, bytes]` and a method
  `fail_next(exc: Exception) -> None`.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_object_store_fake.py`:

```python
import pytest


async def test_get_returns_body_and_etag():
    from storage.fake import FakeObjectStore

    store = FakeObjectStore({"config/prices.json": b'{"a": 1}'})

    obj = await store.get("config/prices.json")

    assert obj is not None
    assert obj.body == b'{"a": 1}'
    assert obj.etag


async def test_get_returns_none_for_a_missing_key():
    from storage.fake import FakeObjectStore

    assert await FakeObjectStore().get("nope") is None


async def test_get_if_none_match_returns_the_sentinel_when_the_etag_matches():
    from storage.object_store import NOT_MODIFIED
    from storage.fake import FakeObjectStore

    store = FakeObjectStore({"k": b"v"})
    first = await store.get("k")

    assert await store.get_if_none_match("k", first.etag) is NOT_MODIFIED


async def test_get_if_none_match_returns_the_object_when_the_etag_differs():
    from storage.fake import FakeObjectStore

    store = FakeObjectStore({"k": b"v"})

    obj = await store.get_if_none_match("k", '"stale"')

    assert obj.body == b"v"


async def test_put_changes_the_etag():
    from storage.fake import FakeObjectStore

    store = FakeObjectStore({"k": b"v1"})
    before = (await store.get("k")).etag

    await store.put("k", b"v2", "application/json")
    after = await store.get("k")

    assert after.body == b"v2"
    assert after.etag != before
    assert store.puts["k"] == b"v2"


async def test_fail_next_raises_once_then_recovers():
    from storage.fake import FakeObjectStore

    store = FakeObjectStore({"k": b"v"})
    store.fail_next(RuntimeError("boom"))

    with pytest.raises(RuntimeError):
        await store.get("k")

    assert (await store.get("k")).body == b"v"


async def test_get_calls_counts_network_reads():
    from storage.fake import FakeObjectStore

    store = FakeObjectStore({"k": b"v"})
    await store.get("k")
    await store.get("k")

    assert store.get_calls == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/unit/test_object_store_fake.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'storage'`

- [ ] **Step 3: Write the Protocol**

Create `storage/object_store.py`:

```python
from enum import Enum, auto
from typing import NamedTuple, Protocol


class ObjectData(NamedTuple):
    """One object's bytes and the ETag the store reported for them."""

    body: bytes
    etag: str


class NotModified(Enum):
    """
    Single-member enum so the sentinel has a type a checker can narrow.

    `ObjectData | NotModified | None` is three distinct outcomes — changed,
    unchanged, gone — and a bare object() would collapse into `Any`.
    """

    token = auto()


NOT_MODIFIED = NotModified.token


class ObjectStore(Protocol):
    """
    The object operations this service needs. Four methods, deliberately.

    Callers depend on this, not on aioboto3, which is what lets every unit
    test run against an in-memory fake with no Docker.
    """

    async def get(self, key: str) -> ObjectData | None:
        """The object, or None when the key does not exist."""
        ...

    async def get_if_none_match(
        self, key: str, etag: str
    ) -> ObjectData | NotModified | None:
        """NOT_MODIFIED when the ETag still matches; the object when it does not."""
        ...

    async def put(self, key: str, body: bytes, content_type: str) -> None:
        """Write an object, overwriting whatever was there."""
        ...

    async def head_bucket(self) -> None:
        """Raise if the bucket is unreachable. Used by the health probe."""
        ...
```

- [ ] **Step 4: Write the fake**

Create `storage/fake.py`:

```python
import hashlib

from storage.object_store import NOT_MODIFIED, ObjectData, NotModified


def _etag(body: bytes) -> str:
    """S3 quotes its ETags, and code that strips quotes should be exercised."""
    return f'"{hashlib.md5(body).hexdigest()}"'


class FakeObjectStore:
    """
    In-memory ObjectStore for unit tests.

    Records enough to assert on: how many reads reached the "network"
    (`get_calls`), what was written (`puts`), and an injectable failure
    (`fail_next`) so both branches of the error classifier can be driven
    without a container.
    """

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self._objects: dict[str, bytes] = dict(objects or {})
        self.get_calls = 0
        self.puts: dict[str, bytes] = {}
        self._next_error: Exception | None = None

    def fail_next(self, exc: Exception) -> None:
        """The next call — any method — raises `exc`, then behaviour resumes."""
        self._next_error = exc

    def _maybe_fail(self) -> None:
        if self._next_error is not None:
            error, self._next_error = self._next_error, None
            raise error

    async def get(self, key: str) -> ObjectData | None:
        self._maybe_fail()
        self.get_calls += 1
        body = self._objects.get(key)
        if body is None:
            return None
        return ObjectData(body=body, etag=_etag(body))

    async def get_if_none_match(
        self, key: str, etag: str
    ) -> ObjectData | NotModified | None:
        obj = await self.get(key)
        if obj is None:
            return None
        if obj.etag == etag:
            return NOT_MODIFIED
        return obj

    async def put(self, key: str, body: bytes, content_type: str) -> None:
        self._maybe_fail()
        self._objects[key] = body
        self.puts[key] = body

    async def head_bucket(self) -> None:
        self._maybe_fail()
```

- [ ] **Step 5: Write the package exports**

Create `storage/__init__.py`:

```python
from storage.object_store import NOT_MODIFIED, NotModified, ObjectData, ObjectStore

__all__ = ["NOT_MODIFIED", "NotModified", "ObjectData", "ObjectStore"]
```

- [ ] **Step 6: Run test to verify it passes**

Run: `poetry run pytest tests/unit/test_object_store_fake.py -v`
Expected: PASS (7 passed)

- [ ] **Step 7: Commit**

```bash
make fmt && make lint
git add storage/__init__.py storage/object_store.py storage/fake.py tests/unit/test_object_store_fake.py
git commit -m "feat: add the ObjectStore protocol and an in-memory fake"
```

---

### Task 3: The error classifier

**Files:**
- Create: `storage/errors.py`
- Test: `tests/unit/test_storage_errors.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `PermanentHandlerError(Exception)`; `is_retryable(exc: BaseException) -> bool`.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_storage_errors.py`:

```python
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "GetObject")


def test_transport_failures_are_retryable():
    from storage.errors import is_retryable

    assert is_retryable(EndpointConnectionError(endpoint_url="http://x"))
    assert is_retryable(ConnectTimeoutError(endpoint_url="http://x"))
    assert is_retryable(ReadTimeoutError(endpoint_url="http://x"))


def test_server_side_client_errors_are_retryable():
    from storage.errors import is_retryable

    for code in ("500", "503", "SlowDown", "InternalError", "ServiceUnavailable"):
        assert is_retryable(_client_error(code)), code


def test_missing_or_forbidden_objects_are_permanent():
    from storage.errors import is_retryable

    for code in ("NoSuchKey", "NoSuchBucket", "AccessDenied", "404"):
        assert not is_retryable(_client_error(code)), code


def test_permanent_handler_error_is_not_retryable():
    from storage.errors import PermanentHandlerError, is_retryable

    assert not is_retryable(PermanentHandlerError("bad catalog"))


def test_an_unknown_exception_is_not_retryable():
    from storage.errors import is_retryable

    assert not is_retryable(ValueError("something else"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/unit/test_storage_errors.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'storage.errors'`

- [ ] **Step 3: Write the implementation**

Create `storage/errors.py`:

```python
from botocore.exceptions import (
    ClientError,
    ConnectionError as BotocoreConnectionError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)


class PermanentHandlerError(Exception):
    """
    A failure that will fail identically on every retry.

    The consumer dead-letters this on sight instead of spending
    CONSUMER_MAX_RETRIES attempts reaching the same conclusion. Raise it for
    a missing or malformed price catalog, or an item the catalog does not
    price.
    """


# Transport-level failures: the call never reached a decision.
RETRYABLE_EXCEPTIONS = (
    EndpointConnectionError,
    ConnectTimeoutError,
    ReadTimeoutError,
    BotocoreConnectionError,
)

# ClientError covers both "the server is having a bad minute" and "this key
# does not exist", so the class alone cannot decide — only the code can.
RETRYABLE_CODES = frozenset(
    {
        "500",
        "502",
        "503",
        "504",
        "InternalError",
        "ServiceUnavailable",
        "SlowDown",
        "RequestTimeout",
        "RequestTimeTooSkewed",
    }
)


def is_retryable(exc: BaseException) -> bool:
    """
    True when the same call might succeed later.

    Mirrors messaging/outbox/backoff.py for Redis. The trap here is different:
    isinstance(exc, ClientError) is true for a 503 and for a NoSuchKey alike,
    so this branches on the response code. Anything unrecognised is treated as
    permanent — retrying a bug three times only delays the dead letter.
    """
    if isinstance(exc, RETRYABLE_EXCEPTIONS):
        return True
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        return code in RETRYABLE_CODES
    return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/unit/test_storage_errors.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
make fmt && make lint
git add storage/errors.py tests/unit/test_storage_errors.py
git commit -m "feat: classify S3 failures as retryable or permanent"
```

---

### Task 4: The S3-backed store

**Files:**
- Create: `storage/s3/__init__.py`
- Create: `storage/s3/client.py`
- Modify: `storage/__init__.py`
- Modify: `tests/conftest.py`
- Test: `tests/integration/test_s3_object_store.py`

**Interfaces:**
- Consumes: `ObjectData`, `NOT_MODIFIED`, `NotModified` from `storage.object_store`.
- Produces: `S3ObjectStore` (an `ObjectStore` with `async def open()` and
  `async def close()`); `get_object_store() -> S3ObjectStore`;
  `async close_object_store() -> None`; conftest fixtures `seaweedfs_url`
  (session, str) and `object_store` (function, an opened `S3ObjectStore`).

- [ ] **Step 1: Write the failing test**

Create `tests/integration/test_s3_object_store.py`:

```python
import pytest

from storage.object_store import NOT_MODIFIED

pytestmark = pytest.mark.integration


async def test_put_then_get_round_trips(object_store):
    await object_store.put("t/a.json", b'{"x": 1}', "application/json")

    obj = await object_store.get("t/a.json")

    assert obj.body == b'{"x": 1}'
    assert obj.etag


async def test_get_returns_none_for_a_missing_key(object_store):
    assert await object_store.get("t/does-not-exist.json") is None


async def test_conditional_get_reports_not_modified(object_store):
    await object_store.put("t/b.json", b"v1", "application/json")
    first = await object_store.get("t/b.json")

    assert await object_store.get_if_none_match("t/b.json", first.etag) is NOT_MODIFIED


async def test_conditional_get_returns_the_new_body_after_a_write(object_store):
    await object_store.put("t/c.json", b"v1", "application/json")
    first = await object_store.get("t/c.json")
    await object_store.put("t/c.json", b"v2", "application/json")

    obj = await object_store.get_if_none_match("t/c.json", first.etag)

    assert obj.body == b"v2"


async def test_conditional_get_returns_none_for_a_missing_key(object_store):
    assert await object_store.get_if_none_match("t/gone.json", '"x"') is None


async def test_head_bucket_succeeds_for_the_configured_bucket(object_store):
    await object_store.head_bucket()
```

- [ ] **Step 2: Add the fixtures**

In `tests/conftest.py`, add these three fixtures after `redis_url`:

```python
@pytest.fixture(scope="session")
def seaweedfs_url() -> str:
    """
    Start SeaweedFS with its S3 gateway and yield the endpoint URL.

    Deliberately the image docker-compose.yaml runs, not MinIO: an integration
    test that passes against a different S3 implementation than the one the
    template ships proves less than it appears to.
    """
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    container = (
        DockerContainer("chrislusf/seaweedfs:4.44")
        .with_command(
            "server -dir=/data -s3 -s3.port=8333 -volume.max=100 "
            "-master.volumeSizeLimitMB=100 -master.volumePreallocate=false"
        )
        .with_exposed_ports(8333)
    )
    with container:
        wait_for_logs(container, "Start Seaweed S3 API Server", timeout=90)
        host = container.get_container_host_ip()
        port = container.get_exposed_port(8333)
        yield f"http://{host}:{port}"


@pytest.fixture(scope="session")
def s3_settings(app_settings, seaweedfs_url: str):
    """
    Point the cached settings at the SeaweedFS container.

    Anonymous credentials: the container runs without an -s3.config file, so
    it accepts any key. The values still have to be set, because botocore
    refuses to sign a request with no credentials at all.
    """
    from config.settings import get_settings

    os.environ["S3_ENDPOINT_URL"] = seaweedfs_url
    os.environ["S3_BUCKET"] = "test-bucket"
    os.environ["S3_ACCESS_KEY_ID"] = "test"
    os.environ["S3_SECRET_ACCESS_KEY"] = "test"
    os.environ["PRICES_CACHE_TTL_S"] = "1"
    get_settings.cache_clear()
    yield get_settings()
    get_settings.cache_clear()


@pytest_asyncio.fixture
async def object_store(s3_settings):
    """
    An opened S3ObjectStore with an empty bucket.

    Function-scoped because aioboto3's client binds to the event loop that
    created it, and pytest-asyncio gives each test its own loop — the same
    constraint _reset_session_maker_globals handles for SQLAlchemy.
    """
    from storage.s3.client import S3ObjectStore

    store = S3ObjectStore()
    await store.open()
    await store.ensure_bucket()
    try:
        yield store
    finally:
        await store.close()
        import storage.s3.client as client_module

        client_module._store = None
```

- [ ] **Step 3: Run test to verify it fails**

Run: `poetry run pytest tests/integration/test_s3_object_store.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'storage.s3'`

- [ ] **Step 4: Write the implementation**

Create `storage/s3/__init__.py` (empty file), then `storage/s3/client.py`:

```python
import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config.logging import get_logger
from config.settings import get_settings
from storage.object_store import NOT_MODIFIED, ObjectData, NotModified

logger = get_logger(__name__)

# Codes SeaweedFS and S3 use for "no such object". A conditional GET that
# 304s also arrives as a ClientError, which is why these are matched by code.
_MISSING_CODES = {"NoSuchKey", "404", "NoSuchBucket"}
_NOT_MODIFIED_CODES = {"304", "NotModified"}


class S3ObjectStore:
    """
    ObjectStore backed by any S3-compatible endpoint (SeaweedFS here).

    The client is opened once and held for the process's life, the way the
    Redis producer holds its connection. botocore's internal retries are
    disabled: retry policy for this service lives in the consumer, and two
    layers of it would multiply into a much longer stall than either intends.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self.bucket = settings.S3_BUCKET
        self._session = aioboto3.Session(
            aws_access_key_id=settings.S3_ACCESS_KEY_ID,
            aws_secret_access_key=settings.S3_SECRET_ACCESS_KEY,
            region_name=settings.S3_REGION,
        )
        self._config = Config(
            connect_timeout=settings.S3_CONNECT_TIMEOUT_S,
            read_timeout=settings.S3_READ_TIMEOUT_S,
            retries={"max_attempts": 1, "mode": "standard"},
            signature_version="s3v4",
        )
        self._endpoint_url = settings.S3_ENDPOINT_URL
        self._context = None
        self._client = None

    async def open(self) -> None:
        """Enter the client context. Called from the application lifespan."""
        if self._client is not None:
            return
        self._context = self._session.client(
            "s3", endpoint_url=self._endpoint_url, config=self._config
        )
        self._client = await self._context.__aenter__()
        logger.info("S3 client opened", endpoint=self._endpoint_url, bucket=self.bucket)

    async def close(self) -> None:
        if self._context is not None:
            await self._context.__aexit__(None, None, None)
        self._context = None
        self._client = None
        logger.debug("S3 client closed")

    async def ensure_bucket(self) -> None:
        """Create the bucket if absent. For tests and local bootstrapping."""
        try:
            await self._client.head_bucket(Bucket=self.bucket)
        except ClientError:
            await self._client.create_bucket(Bucket=self.bucket)

    async def get(self, key: str) -> ObjectData | None:
        try:
            response = await self._client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code", "") in _MISSING_CODES:
                return None
            raise
        body = await response["Body"].read()
        return ObjectData(body=body, etag=response["ETag"])

    async def get_if_none_match(
        self, key: str, etag: str
    ) -> ObjectData | NotModified | None:
        try:
            response = await self._client.get_object(
                Bucket=self.bucket, Key=key, IfNoneMatch=etag
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            # botocore raises on a 304; it is a successful answer, not an error.
            if code in _NOT_MODIFIED_CODES or status == 304:
                return NOT_MODIFIED
            if code in _MISSING_CODES:
                return None
            raise
        body = await response["Body"].read()
        return ObjectData(body=body, etag=response["ETag"])

    async def put(self, key: str, body: bytes, content_type: str) -> None:
        await self._client.put_object(
            Bucket=self.bucket, Key=key, Body=body, ContentType=content_type
        )
        logger.debug("Object written", key=key, bytes=len(body))

    async def head_bucket(self) -> None:
        await self._client.head_bucket(Bucket=self.bucket)


_store: S3ObjectStore | None = None


def get_object_store() -> S3ObjectStore:
    """
    Process-wide store. Lazily constructed so importing this module does not
    open a socket — the same contract as get_producer().

    The returned store is unusable until open() has run; the lifespan does
    that before the consumer starts.
    """
    global _store
    if _store is None:
        _store = S3ObjectStore()
    return _store


async def close_object_store() -> None:
    """Dispose of the process-wide store. Called from the app lifespan."""
    global _store
    if _store is not None:
        await _store.close()
        _store = None
```

- [ ] **Step 5: Export it**

Replace `storage/__init__.py` with:

```python
from storage.errors import PermanentHandlerError, is_retryable
from storage.object_store import NOT_MODIFIED, NotModified, ObjectData, ObjectStore
from storage.s3.client import close_object_store, get_object_store

__all__ = [
    "NOT_MODIFIED",
    "NotModified",
    "ObjectData",
    "ObjectStore",
    "PermanentHandlerError",
    "close_object_store",
    "get_object_store",
    "is_retryable",
]
```

- [ ] **Step 6: Run test to verify it passes**

Run: `poetry run pytest tests/integration/test_s3_object_store.py -v`
Expected: PASS (6 passed). First run pulls the SeaweedFS image — allow a few minutes.

- [ ] **Step 7: Verify the unit suite still runs without Docker**

Run: `poetry run pytest -m "not integration" -q`
Expected: PASS, no container started.

- [ ] **Step 8: Commit**

```bash
make fmt && make lint
git add storage/ tests/conftest.py tests/integration/test_s3_object_store.py
git commit -m "feat: add the aioboto3-backed object store"
```

---

### Task 5: total_cents on orders

**Files:**
- Create: `db/migrations/versions/0004_add_order_total_cents.py`
- Modify: `db/models.py`
- Modify: `db/repositories/order_repository.py`
- Test: `tests/integration/test_order_repository.py` (extend)

**Interfaces:**
- Consumes: nothing.
- Produces: `Order.total_cents: int | None`;
  `OrderRepository.confirm(order_ref: str, total_cents: int | None = None) -> bool`;
  `OrderRepository.list_confirmed_created_on(day: date) -> list[Order]`.

- [ ] **Step 1: Write the failing test**

Append to `tests/integration/test_order_repository.py`:

```python
async def test_confirm_stores_the_total_in_cents(db_session):
    from db.repositories.order_repository import OrderRepository

    repo = OrderRepository(db_session)
    await repo.create(order_ref="tc-1", item="widget", quantity=3)
    await db_session.commit()

    confirmed = await repo.confirm("tc-1", total_cents=5997)
    await db_session.commit()

    order = await repo.get_by_ref("tc-1")
    assert confirmed is True
    assert order.total_cents == 5997
    assert isinstance(order.total_cents, int)


async def test_confirm_on_an_already_confirmed_order_leaves_the_total_alone(db_session):
    from db.repositories.order_repository import OrderRepository

    repo = OrderRepository(db_session)
    await repo.create(order_ref="tc-2", item="widget", quantity=1)
    await db_session.commit()
    await repo.confirm("tc-2", total_cents=1999)
    await db_session.commit()

    again = await repo.confirm("tc-2", total_cents=9999)
    await db_session.commit()

    order = await repo.get_by_ref("tc-2")
    assert again is False
    assert order.total_cents == 1999


async def test_list_confirmed_created_on_filters_by_creation_date(db_session):
    from datetime import UTC, datetime, timedelta

    from db.repositories.order_repository import OrderRepository

    repo = OrderRepository(db_session)
    today = datetime.now(UTC)
    yesterday = today - timedelta(days=1)

    order_a = await repo.create(order_ref="lc-1", item="widget", quantity=1)
    order_b = await repo.create(order_ref="lc-2", item="widget", quantity=2)
    order_b.created_at = yesterday
    await db_session.commit()
    await repo.confirm("lc-1", total_cents=1999)
    await repo.confirm("lc-2", total_cents=3998)
    await db_session.commit()

    rows = await repo.list_confirmed_created_on(today.date())

    assert [o.order_ref for o in rows] == ["lc-1"]


async def test_list_confirmed_created_on_excludes_pending_orders(db_session):
    from datetime import UTC, datetime

    from db.repositories.order_repository import OrderRepository

    repo = OrderRepository(db_session)
    await repo.create(order_ref="lc-3", item="widget", quantity=1)
    await db_session.commit()

    rows = await repo.list_confirmed_created_on(datetime.now(UTC).date())

    assert rows == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/integration/test_order_repository.py -v -k "total or list_confirmed"`
Expected: FAIL — `TypeError: confirm() got an unexpected keyword argument 'total_cents'`

- [ ] **Step 3: Add the column to the model**

In `db/models.py`, add `total_cents` to `Order` immediately after `quantity`:

```python
    # Money is stored in cents as an integer — never a float, never a
    # NUMERIC that invites Decimal round-tripping. BIGINT rather than INT
    # because a 32-bit column caps out near $21M.
    total_cents: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
```

`BigInteger` is already imported at the top of the file.

- [ ] **Step 4: Write the migration**

Create `db/migrations/versions/0004_add_order_total_cents.py`:

```python
"""add total_cents to orders

Revision ID: 0004
Revises: 0003
"""

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("orders", sa.Column("total_cents", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("orders", "total_cents")
```

Confirm the `revision`/`down_revision` identifiers match the style of
`db/migrations/versions/0003_create_outbox.py`; copy its exact convention if
it differs.

- [ ] **Step 5: Update the repository**

In `db/repositories/order_repository.py`, add `date` to the datetime import
(`from datetime import UTC, date, datetime`), then replace `confirm` and add
`list_confirmed_created_on`:

```python
    async def confirm(self, order_ref: str, total_cents: int | None = None) -> bool:
        """
        Conditional confirm. Returns False when the order does not exist or
        was already confirmed, which makes replayed events harmless.

        The total is written in the same UPDATE, so an order can never be
        `confirmed` with no price — and a replay cannot overwrite the price
        the first delivery computed, because the WHERE clause excludes it.
        """
        result = await self.session.execute(
            update(Order)
            .where(Order.order_ref == order_ref, Order.status == "pending")
            .values(
                status="confirmed",
                total_cents=total_cents,
                confirmed_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
        return result.rowcount > 0

    async def list_confirmed_created_on(self, day: date) -> list[Order]:
        """
        Confirmed orders *created* on `day` (UTC), oldest confirmation first.

        Created, not confirmed: an order placed at 23:59 and confirmed at
        00:01 must stay in the rollup for the day it was placed, or a rebuild
        would drop it from one file without adding it to another.
        """
        start = datetime.combine(day, time.min, tzinfo=UTC)
        end = start + timedelta(days=1)
        result = await self.session.execute(
            select(Order)
            .where(
                Order.status == "confirmed",
                Order.created_at >= start,
                Order.created_at < end,
            )
            .order_by(Order.confirmed_at)
        )
        return list(result.scalars().all())
```

Extend the datetime import to `from datetime import UTC, date, datetime, time, timedelta`.

- [ ] **Step 6: Run the tests**

Run: `poetry run pytest tests/integration/test_order_repository.py tests/integration/test_migrations.py -v`
Expected: PASS — including the migration round-trip, which exercises `downgrade()`.

- [ ] **Step 7: Commit**

```bash
make fmt && make lint
git add db/models.py db/migrations/versions/0004_add_order_total_cents.py db/repositories/order_repository.py tests/integration/test_order_repository.py
git commit -m "feat: store the order total in cents"
```

---

### Task 6: The price catalog

**Files:**
- Create: `core/services/pricing.py`
- Modify: `core/services/__init__.py`
- Test: `tests/unit/test_pricing.py`

**Interfaces:**
- Consumes: `ObjectStore`, `NOT_MODIFIED` (Task 2), `PermanentHandlerError` (Task 3),
  `Settings.S3_PRICES_KEY` and `PRICES_CACHE_TTL_S` (Task 1).
- Produces: `PriceItem` (Pydantic, `unit_price_cents: int`);
  `PriceCatalogDocument` (Pydantic, `currency: str`, `items: dict[str, PriceItem]`,
  method `unit_price_cents(item: str) -> int`);
  `PriceCatalog(store: ObjectStore, *, ttl_s: int | None = None, key: str | None = None)`
  with `async get() -> PriceCatalogDocument`;
  `get_price_catalog() -> PriceCatalog`; `reset_price_catalog() -> None`.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_pricing.py`:

```python
import asyncio
import json

import pytest

from storage.errors import PermanentHandlerError
from storage.fake import FakeObjectStore

CATALOG = json.dumps(
    {"currency": "USD", "items": {"widget": {"unit_price_cents": 1999}}}
).encode()


def _catalog(store, ttl_s=60):
    from core.services.pricing import PriceCatalog

    return PriceCatalog(store, ttl_s=ttl_s, key="config/prices.json")


async def test_first_get_loads_from_the_store():
    store = FakeObjectStore({"config/prices.json": CATALOG})

    doc = await _catalog(store).get()

    assert doc.currency == "USD"
    assert doc.unit_price_cents("widget") == 1999
    assert store.get_calls == 1


async def test_a_second_get_inside_the_ttl_does_not_touch_the_store():
    store = FakeObjectStore({"config/prices.json": CATALOG})
    catalog = _catalog(store)

    await catalog.get()
    await catalog.get()

    assert store.get_calls == 1


async def test_after_the_ttl_an_unchanged_object_is_revalidated_not_reloaded():
    store = FakeObjectStore({"config/prices.json": CATALOG})
    catalog = _catalog(store, ttl_s=0)

    first = await catalog.get()
    second = await catalog.get()

    assert store.get_calls == 2
    # A 304 keeps the parsed object rather than building a new one.
    assert second is first


async def test_after_the_ttl_a_changed_object_replaces_the_cache():
    store = FakeObjectStore({"config/prices.json": CATALOG})
    catalog = _catalog(store, ttl_s=0)
    await catalog.get()

    updated = json.dumps(
        {"currency": "USD", "items": {"widget": {"unit_price_cents": 2500}}}
    ).encode()
    await store.put("config/prices.json", updated, "application/json")

    assert (await catalog.get()).unit_price_cents("widget") == 2500


async def test_concurrent_callers_cause_one_load():
    store = FakeObjectStore({"config/prices.json": CATALOG})
    catalog = _catalog(store)

    await asyncio.gather(*(catalog.get() for _ in range(5)))

    assert store.get_calls == 1


async def test_a_missing_catalog_is_permanent():
    catalog = _catalog(FakeObjectStore())

    with pytest.raises(PermanentHandlerError, match="not found"):
        await catalog.get()


async def test_a_non_json_catalog_is_permanent():
    catalog = _catalog(FakeObjectStore({"config/prices.json": b"not json"}))

    with pytest.raises(PermanentHandlerError):
        await catalog.get()


async def test_a_fractional_price_is_permanent():
    body = json.dumps(
        {"currency": "USD", "items": {"widget": {"unit_price_cents": 19.99}}}
    ).encode()
    catalog = _catalog(FakeObjectStore({"config/prices.json": body}))

    with pytest.raises(PermanentHandlerError):
        await catalog.get()


async def test_an_unpriced_item_is_permanent():
    doc = await _catalog(FakeObjectStore({"config/prices.json": CATALOG})).get()

    with pytest.raises(PermanentHandlerError, match="gizmo"):
        doc.unit_price_cents("gizmo")


async def test_a_failed_revalidation_leaves_the_cache_intact():
    store = FakeObjectStore({"config/prices.json": CATALOG})
    catalog = _catalog(store, ttl_s=0)
    await catalog.get()

    store.fail_next(RuntimeError("s3 down"))
    with pytest.raises(RuntimeError):
        await catalog.get()

    # The store recovers; the cached catalog was never discarded.
    assert (await catalog.get()).unit_price_cents("widget") == 1999
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/unit/test_pricing.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'core.services.pricing'`

- [ ] **Step 3: Write the implementation**

Create `core/services/pricing.py`:

```python
import asyncio
import time

from pydantic import BaseModel, Field, StrictInt, ValidationError

from config.logging import get_logger
from config.settings import get_settings
from storage.errors import PermanentHandlerError
from storage.object_store import NOT_MODIFIED, ObjectStore

logger = get_logger(__name__)


class PriceItem(BaseModel):
    """One priced item. StrictInt so 19.99 fails here, not silently truncates."""

    unit_price_cents: StrictInt = Field(ge=0)


class PriceCatalogDocument(BaseModel):
    """The parsed contents of config/prices.json."""

    currency: str
    items: dict[str, PriceItem]

    def unit_price_cents(self, item: str) -> int:
        entry = self.items.get(item)
        if entry is None:
            raise PermanentHandlerError(f"no price for item: {item}")
        return entry.unit_price_cents


class PriceCatalog:
    """
    The price catalog, cached in process and revalidated with its ETag.

    Config in an object store is read far more often than it changes, so a
    fresh GET per message is waste. After the TTL the next caller sends a
    conditional GET: a 304 costs a round trip and no parse, a 200 replaces
    the cached document. A failed revalidation propagates without discarding
    what is already held — a file that becomes briefly unreachable should not
    invalidate a copy that is almost certainly still correct.
    """

    def __init__(
        self,
        store: ObjectStore,
        *,
        ttl_s: int | None = None,
        key: str | None = None,
    ) -> None:
        settings = get_settings()
        self._store = store
        self._ttl_s = settings.PRICES_CACHE_TTL_S if ttl_s is None else ttl_s
        self._key = key or settings.S3_PRICES_KEY
        self._document: PriceCatalogDocument | None = None
        self._etag: str | None = None
        self._loaded_at = 0.0
        self._lock = asyncio.Lock()

    async def get(self) -> PriceCatalogDocument:
        """The catalog, from cache when fresh, revalidated when not."""
        if self._is_fresh():
            return self._document

        async with self._lock:
            # A caller that waited on the lock may find the work already done.
            if self._is_fresh():
                return self._document
            await self._refresh()
            return self._document

    def _is_fresh(self) -> bool:
        return (
            self._document is not None
            and (time.monotonic() - self._loaded_at) < self._ttl_s
        )

    async def _refresh(self) -> None:
        if self._etag is not None:
            result = await self._store.get_if_none_match(self._key, self._etag)
            if result is NOT_MODIFIED:
                self._loaded_at = time.monotonic()
                logger.debug("Price catalog unchanged", key=self._key)
                return
        else:
            result = await self._store.get(self._key)

        if result is None:
            raise PermanentHandlerError(f"price catalog not found: {self._key}")

        self._document = self._parse(result.body)
        self._etag = result.etag
        self._loaded_at = time.monotonic()
        logger.info(
            "Price catalog loaded",
            key=self._key,
            items=len(self._document.items),
            etag=self._etag,
        )

    def _parse(self, body: bytes) -> PriceCatalogDocument:
        try:
            return PriceCatalogDocument.model_validate_json(body)
        except ValidationError as e:
            # A malformed catalog fails identically on every retry.
            raise PermanentHandlerError(
                f"price catalog at {self._key} is invalid: {e}"
            ) from e


_catalog: PriceCatalog | None = None


def get_price_catalog() -> PriceCatalog:
    """
    Process-wide catalog, so the cache is shared across messages.

    Bound to the process-wide object store, which the lifespan opens.
    """
    global _catalog
    if _catalog is None:
        from storage.s3.client import get_object_store

        _catalog = PriceCatalog(get_object_store())
    return _catalog


def reset_price_catalog() -> None:
    """Drop the cached catalog. Called from the app lifespan on shutdown."""
    global _catalog
    _catalog = None
```

- [ ] **Step 4: Export it**

In `core/services/__init__.py`, add to the existing imports and `__all__`:

```python
from core.services.pricing import (
    PriceCatalog,
    PriceCatalogDocument,
    get_price_catalog,
    reset_price_catalog,
)
```

Add `"PriceCatalog"`, `"PriceCatalogDocument"`, `"get_price_catalog"` and
`"reset_price_catalog"` to `__all__`.

- [ ] **Step 5: Run test to verify it passes**

Run: `poetry run pytest tests/unit/test_pricing.py -v`
Expected: PASS (10 passed)

- [ ] **Step 6: Commit**

```bash
make fmt && make lint
git add core/services/pricing.py core/services/__init__.py tests/unit/test_pricing.py
git commit -m "feat: read the price catalog from S3 with an ETag-revalidated cache"
```

---

### Task 7: The rollup projection

**Files:**
- Create: `core/services/rollup_service.py`
- Modify: `core/services/__init__.py`
- Test: `tests/unit/test_rollup_service.py`

**Interfaces:**
- Consumes: `ObjectStore` (Task 2), `OrderRepository.list_confirmed_created_on`
  (Task 5), `Settings.rollup_key` (Task 1).
- Produces: `RollupService(session: AsyncSession, store: ObjectStore)` with
  `async rebuild_for_date(day: date, *, currency: str = "USD") -> str`
  returning the key written.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_rollup_service.py`:

```python
import json
from datetime import UTC, date, datetime
from types import SimpleNamespace

from storage.fake import FakeObjectStore


class _StubRepo:
    """Stands in for OrderRepository; RollupService only calls one method."""

    def __init__(self, orders):
        self._orders = orders

    async def list_confirmed_created_on(self, day):
        return self._orders


def _order(ref, item, qty, total_cents):
    return SimpleNamespace(
        order_ref=ref,
        item=item,
        quantity=qty,
        total_cents=total_cents,
        confirmed_at=datetime(2026, 9, 7, 12, 0, tzinfo=UTC),
    )


def _service(store, orders):
    from core.services.rollup_service import RollupService

    service = RollupService(session=None, store=store)
    service.repo = _StubRepo(orders)
    return service


async def test_rebuild_writes_the_expected_document():
    store = FakeObjectStore()
    orders = [_order("a", "widget", 3, 5997), _order("b", "gadget", 1, 4550)]

    key = await _service(store, orders).rebuild_for_date(date(2026, 9, 7))

    assert key == "rollups/2026-09-07.json"
    document = json.loads(store.puts[key])
    assert document == {
        "date": "2026-09-07",
        "currency": "USD",
        "order_count": 2,
        "total_cents": 10547,
        "orders": [
            {
                "order_ref": "a",
                "item": "widget",
                "quantity": 3,
                "unit_price_cents": 1999,
                "total_cents": 5997,
            },
            {
                "order_ref": "b",
                "item": "gadget",
                "quantity": 1,
                "unit_price_cents": 4550,
                "total_cents": 4550,
            },
        ],
    }


async def test_rebuild_writes_an_empty_document_when_there_are_no_orders():
    store = FakeObjectStore()

    key = await _service(store, []).rebuild_for_date(date(2026, 9, 7))

    document = json.loads(store.puts[key])
    assert document["orders"] == []
    assert document["total_cents"] == 0
    assert document["order_count"] == 0


async def test_rebuild_overwrites_rather_than_merging():
    store = FakeObjectStore({"rollups/2026-09-07.json": b'{"stale": true}'})

    await _service(store, [_order("a", "widget", 1, 1999)]).rebuild_for_date(
        date(2026, 9, 7)
    )

    document = json.loads(store.puts["rollups/2026-09-07.json"])
    assert "stale" not in document
    assert store.get_calls == 0  # the projection never reads the old object


async def test_rebuild_skips_orders_with_no_total():
    store = FakeObjectStore()
    orders = [_order("a", "widget", 1, 1999), _order("b", "gadget", 1, None)]

    await _service(store, orders).rebuild_for_date(date(2026, 9, 7))

    document = json.loads(store.puts["rollups/2026-09-07.json"])
    assert [o["order_ref"] for o in document["orders"]] == ["a"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/unit/test_rollup_service.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'core.services.rollup_service'`

- [ ] **Step 3: Write the implementation**

Create `core/services/rollup_service.py`:

```python
import json
from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from config.logging import get_logger
from config.settings import get_settings
from db.repositories.order_repository import OrderRepository
from storage.object_store import ObjectStore

logger = get_logger(__name__)


class RollupService:
    """
    Writes the daily rollup object.

    The object is a projection, not an accumulator: this rebuilds the whole
    day from SQL and overwrites the key. It never reads what is there.

    That is what makes concurrency a non-problem. Several consumers can race
    on the same key; the last write wins, and the winner is correct because
    every writer computed from the same authoritative rows. Merging into the
    existing object instead would need a conditional PUT and a retry loop to
    avoid losing updates.
    """

    def __init__(self, session: AsyncSession, store: ObjectStore) -> None:
        self.session = session
        self.store = store
        self.repo = OrderRepository(session) if session is not None else None

    async def rebuild_for_date(self, day: date, *, currency: str = "USD") -> str:
        orders = await self.repo.list_confirmed_created_on(day)
        rows = [
            {
                "order_ref": order.order_ref,
                "item": order.item,
                "quantity": order.quantity,
                # Derived, not stored: the catalog price at confirmation time
                # is whatever the total implies, which keeps the object
                # consistent with the row even if the catalog changes later.
                "unit_price_cents": order.total_cents // order.quantity,
                "total_cents": order.total_cents,
            }
            for order in orders
            if order.total_cents is not None and order.quantity
        ]
        document = {
            "date": day.isoformat(),
            "currency": currency,
            "order_count": len(rows),
            "total_cents": sum(row["total_cents"] for row in rows),
            "orders": rows,
        }

        key = get_settings().rollup_key(day)
        await self.store.put(
            key,
            json.dumps(document, separators=(",", ":")).encode(),
            "application/json",
        )
        logger.info(
            "Rollup rebuilt",
            key=key,
            order_count=document["order_count"],
            total_cents=document["total_cents"],
        )
        return key
```

- [ ] **Step 4: Export it**

In `core/services/__init__.py`, add `from core.services.rollup_service import RollupService`
and `"RollupService"` to `__all__`.

- [ ] **Step 5: Run test to verify it passes**

Run: `poetry run pytest tests/unit/test_rollup_service.py -v`
Expected: PASS (4 passed)

- [ ] **Step 6: Commit**

```bash
make fmt && make lint
git add core/services/rollup_service.py core/services/__init__.py tests/unit/test_rollup_service.py
git commit -m "feat: rebuild the daily rollup object from SQL"
```

---

### Task 8: Permanent errors in the consumer

**Files:**
- Modify: `messaging/consumer/redis_consumer.py:150-169`
- Test: `tests/unit/test_redis_consumer.py` (extend)

**Interfaces:**
- Consumes: `PermanentHandlerError` (Task 3).
- Produces: dead-letter reason string `"permanent_handler_error"`.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_redis_consumer.py`. Match the file's existing
fixture and mock style — if it builds a consumer through a helper, use that
helper rather than the constructor shown here.

```python
async def test_a_permanent_handler_error_is_dead_lettered_without_retries(monkeypatch):
    from messaging.consumer import redis_consumer as module
    from storage.errors import PermanentHandlerError

    calls = []

    async def failing_dispatch(envelope):
        calls.append(envelope)
        raise PermanentHandlerError("no price for item: gizmo")

    monkeypatch.setattr(module, "dispatch_event", failing_dispatch)

    consumer = _make_consumer()  # existing helper in this test module
    dead_lettered = []
    consumer._dead_letter = _record(dead_lettered)  # existing helper

    await consumer._handle_one("1-0", {"event": _valid_envelope_json()})

    assert len(calls) == 1, "a permanent error must not be retried"
    assert dead_lettered[0]["reason"] == "permanent_handler_error"


async def test_a_generic_handler_error_still_retries(monkeypatch):
    from messaging.consumer import redis_consumer as module

    calls = []

    async def failing_dispatch(envelope):
        calls.append(envelope)
        raise RuntimeError("transient")

    monkeypatch.setattr(module, "dispatch_event", failing_dispatch)

    consumer = _make_consumer()
    dead_lettered = []
    consumer._dead_letter = _record(dead_lettered)

    await consumer._handle_one("1-0", {"event": _valid_envelope_json()})

    assert len(calls) == consumer.max_retries
    assert dead_lettered[0]["reason"] == "handler_error"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/unit/test_redis_consumer.py -v -k permanent`
Expected: FAIL — dispatch called 3 times, reason is `"handler_error"`.

- [ ] **Step 3: Add the branch**

In `messaging/consumer/redis_consumer.py`, import the error near the other
local imports:

```python
from storage.errors import PermanentHandlerError
```

Then, inside the `for attempt in range(1, self.max_retries + 1):` loop in
`_handle_one`, insert this `except` clause **before** the existing
`except Exception as e:` — order matters, since `PermanentHandlerError`
is an `Exception`:

```python
            except PermanentHandlerError as e:
                # No retries: a missing price, a malformed catalog, or an
                # unpriced item fails identically forever. Same treatment
                # ValidationError gets above, for the same reason.
                log.warning("Handler failed permanently", error=str(e))
                await self._dead_letter(
                    message_id,
                    fields,
                    "permanent_handler_error",
                    f"{e}\n{traceback.format_exc()}",
                    attempt,
                )
                return
```

- [ ] **Step 4: Run the tests**

Run: `poetry run pytest tests/unit/test_redis_consumer.py -v`
Expected: PASS — the new cases plus every pre-existing one.

- [ ] **Step 5: Commit**

```bash
make fmt && make lint
git add messaging/consumer/redis_consumer.py tests/unit/test_redis_consumer.py
git commit -m "feat: dead-letter permanent handler errors without retrying"
```

---

### Task 9: Wire the handler

**Files:**
- Modify: `messaging/consumer/handlers/order_created.py`
- Test: `tests/unit/test_order_created_handler_s3.py`

**Interfaces:**
- Consumes: `get_price_catalog()` (Task 6), `RollupService` (Task 7),
  `OrderRepository.confirm(order_ref, total_cents)` (Task 5),
  `get_object_store()` (Task 4).
- Produces: no new public names; `handle(payload, *, correlation_id)` keeps its
  signature.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_order_created_handler_s3.py`:

```python
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from storage.errors import PermanentHandlerError
from storage.fake import FakeObjectStore

CATALOG = json.dumps(
    {"currency": "USD", "items": {"widget": {"unit_price_cents": 1999}}}
).encode()


class _StubRepo:
    def __init__(self, *, confirmed: bool, orders=None):
        self._confirmed = confirmed
        self._orders = orders or []
        self.confirm_calls = []
        self.order = SimpleNamespace(
            order_ref="h-1",
            item="widget",
            quantity=3,
            total_cents=5997,
            created_at=datetime(2026, 9, 7, 23, 59, tzinfo=UTC),
            confirmed_at=datetime(2026, 9, 8, 0, 1, tzinfo=UTC),
        )

    async def confirm(self, order_ref, total_cents=None):
        self.confirm_calls.append((order_ref, total_cents))
        return self._confirmed

    async def get_by_ref(self, order_ref):
        return self.order

    async def list_confirmed_created_on(self, day):
        return self._orders


@pytest.fixture
def wired(monkeypatch):
    """
    Replace the handler's three collaborators: the store, the session, and
    the repository. Returns the store and repo so tests can assert on them.
    """
    from core.services import pricing
    from core.services import rollup_service as rollup_service_module
    from messaging.consumer.handlers import order_created as module

    store = FakeObjectStore({"config/prices.json": CATALOG})
    repo = _StubRepo(confirmed=True)

    pricing._catalog = pricing.PriceCatalog(store, ttl_s=60, key="config/prices.json")
    monkeypatch.setattr(module, "get_object_store", lambda: store)
    monkeypatch.setattr(module, "OrderRepository", lambda session: repo)
    # RollupService builds its own repository from its own module's import,
    # so patching the handler's name alone would leave a real repository
    # talking to the stub session.
    monkeypatch.setattr(rollup_service_module, "OrderRepository", lambda session: repo)

    class _Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def commit(self):
            return None

    monkeypatch.setattr(module, "get_session_maker", lambda: (lambda: _Session()))
    yield SimpleNamespace(store=store, repo=repo, module=module)
    pricing._catalog = None


def _payload(item="widget", quantity=3):
    from messaging.models import OrderCreatedEvent

    return OrderCreatedEvent(order_ref="h-1", item=item, quantity=quantity)


async def test_handler_prices_the_order_and_confirms_it(wired):
    await wired.module.handle(_payload(), correlation_id="h-1")

    assert wired.repo.confirm_calls == [("h-1", 5997)]


async def test_handler_writes_the_rollup(wired):
    wired.repo._orders = [wired.repo.order]

    await wired.module.handle(_payload(), correlation_id="h-1")

    assert json.loads(wired.store.puts["rollups/2026-09-07.json"])["total_cents"] == 5997


async def test_the_rollup_day_comes_from_the_order_not_the_clock(wired):
    """The stub order was created 2026-09-07 23:59 and confirmed after midnight."""
    wired.repo._orders = [wired.repo.order]

    await wired.module.handle(_payload(), correlation_id="h-1")

    assert "rollups/2026-09-07.json" in wired.store.puts
    assert "rollups/2026-09-08.json" not in wired.store.puts


async def test_a_vanished_order_is_permanent(wired):
    wired.repo.order = None

    with pytest.raises(PermanentHandlerError, match="no such order"):
        await wired.module.handle(_payload(), correlation_id="h-1")


async def test_a_redelivery_still_rebuilds_the_rollup(wired):
    # confirm() returns False: the row was already confirmed by an earlier
    # delivery. The rollup must still be written, or a PUT that failed on
    # that earlier delivery would never be retried.
    wired.repo._confirmed = False

    await wired.module.handle(_payload(), correlation_id="h-1")

    assert wired.store.puts, "the rollup must be rebuilt on redelivery too"


async def test_an_unpriced_item_raises_a_permanent_error(wired):
    with pytest.raises(PermanentHandlerError, match="gizmo"):
        await wired.module.handle(_payload(item="gizmo"), correlation_id="h-1")

    assert wired.repo.confirm_calls == []


async def test_a_store_failure_propagates_for_the_consumer_to_classify(wired):
    wired.store.fail_next(RuntimeError("s3 unreachable"))

    with pytest.raises(RuntimeError):
        await wired.module.handle(_payload(), correlation_id="h-1")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/unit/test_order_created_handler_s3.py -v`
Expected: FAIL — `AttributeError: module ... has no attribute 'get_object_store'`

- [ ] **Step 3: Write the implementation**

Replace `messaging/consumer/handlers/order_created.py` with:

```python
from config.logging import get_logger
from core.services.pricing import get_price_catalog
from core.services.rollup_service import RollupService
from db.postgres.session import get_session_maker
from db.repositories.order_repository import OrderRepository
from messaging.models import OrderCreatedEvent
from storage.errors import PermanentHandlerError
from storage.s3.client import get_object_store

logger = get_logger(__name__)


async def handle(payload: OrderCreatedEvent, *, correlation_id: str) -> None:
    """
    Price the order from S3, confirm it, and rebuild the day's rollup object.

    SESSION LIFECYCLE — the thing to copy: a handler has no HTTP request, so
    it cannot use Depends(get_db). It opens its own session from the session
    maker, one per message, and commits it. Do not share a session across
    messages; a failure would poison every later message in the batch.

    IDEMPOTENCY: Redis Streams delivers at least once, so this runs again on
    any redelivery. `confirm()` only touches rows still `pending`, which makes
    a replay a logged no-op instead of an error — and cannot overwrite the
    total the first delivery computed.

    ORDERING: SQL commits before the object is written, and the rollup is
    rebuilt on EVERY delivery, including one whose confirm() was a no-op.
    Skipping the rebuild on a redelivery would look like an optimization and
    would in fact break recovery: if the PUT failed after a successful commit,
    the retry's confirm() affects no rows, and the object would never be
    written at all. Rebuilding unconditionally makes a retry converge.
    """
    log = logger.bind(order_ref=payload.order_ref, correlation_id=correlation_id)

    catalog = await get_price_catalog().get()
    unit_price_cents = catalog.unit_price_cents(payload.item)
    total_cents = unit_price_cents * payload.quantity

    session_maker = get_session_maker()
    async with session_maker() as session:
        repo = OrderRepository(session)
        confirmed = await repo.confirm(payload.order_ref, total_cents=total_cents)
        await session.commit()

        if confirmed:
            log.info("Order confirmed", total_cents=total_cents)
        else:
            log.info("Order already confirmed or missing; rebuilding rollup anyway")

        # The rollup day comes from the ORDER's creation date, not from the
        # clock. A message consumed at 00:01 for an order placed at 23:59
        # belongs to yesterday's file; using today's date would rebuild the
        # wrong object and leave the right one missing that order forever.
        order = await repo.get_by_ref(payload.order_ref)
        if order is None:
            raise PermanentHandlerError(f"no such order: {payload.order_ref}")

        rollups = RollupService(session, get_object_store())
        await rollups.rebuild_for_date(
            order.created_at.date(), currency=catalog.currency
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/unit/test_order_created_handler_s3.py -v`
Expected: PASS (7 passed)

- [ ] **Step 5: Check the existing handler test still passes**

Run: `poetry run pytest tests/integration/test_order_created_handler.py -v`
Expected: it will FAIL — the handler now needs a catalog in S3. Update that
file to use the `object_store` fixture, seed `config/prices.json` into it with
`await object_store.put("config/prices.json", CATALOG, "application/json")`,
and assert `total_cents` on the confirmed row.

- [ ] **Step 6: Commit**

```bash
make fmt && make lint
git add messaging/consumer/handlers/order_created.py tests/unit/test_order_created_handler_s3.py tests/integration/test_order_created_handler.py
git commit -m "feat: price orders from S3 and rebuild the rollup in the handler"
```

---

### Task 10: API surfaces and lifespan

**Files:**
- Create: `api/routes/rollups.py`
- Modify: `api/routes/models.py`
- Modify: `api/routes/health.py`
- Modify: `main.py`
- Test: `tests/integration/test_rollup_routes.py`

**Interfaces:**
- Consumes: `get_object_store`, `close_object_store` (Task 4),
  `reset_price_catalog` (Task 6), `Settings.rollup_key` (Task 1).
- Produces: `GET /rollups/{day}` returning the raw object with
  `Content-Type: application/json`; `OrderResponse.total_cents: int | None`;
  a `"s3"` key in the `/health` `services` object.

- [ ] **Step 1: Write the failing test**

Create `tests/integration/test_rollup_routes.py`:

```python
import json

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.integration


async def test_get_rollup_returns_the_object(object_store, migrated_db):
    from main import app

    document = {"date": "2026-09-07", "currency": "USD", "order_count": 0,
                "total_cents": 0, "orders": []}
    await object_store.put(
        "rollups/2026-09-07.json", json.dumps(document).encode(), "application/json"
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/rollups/2026-09-07")

    assert response.status_code == 200
    assert response.json() == document


async def test_get_rollup_returns_404_when_absent(object_store, migrated_db):
    from main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/rollups/2001-01-01")

    assert response.status_code == 404


async def test_get_rollup_rejects_a_malformed_date(object_store, migrated_db):
    from main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/rollups/not-a-date")

    assert response.status_code == 422


async def test_health_reports_s3(object_store, migrated_db):
    from main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    assert response.json()["services"]["s3"] == "ok"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/integration/test_rollup_routes.py -v`
Expected: FAIL — 404 from FastAPI for an unregistered route.

- [ ] **Step 3: Write the route**

Create `api/routes/rollups.py`:

```python
from datetime import date

from fastapi import APIRouter, HTTPException, Response, status

from config.logging import get_logger
from config.settings import get_settings
from storage.errors import is_retryable
from storage.s3.client import get_object_store

logger = get_logger(__name__)

router = APIRouter(prefix="/rollups", tags=["rollups"])


@router.get("/{day}", summary="The daily order rollup, straight from S3")
async def get_rollup(day: date) -> Response:
    """
    Stream one day's rollup object.

    The body is returned verbatim rather than parsed and re-serialized: the
    object is the artifact, and re-encoding it would hide a malformed write
    instead of surfacing it. FastAPI's `date` conversion gives a 422 for a
    malformed path segment for free.
    """
    key = get_settings().rollup_key(day)
    try:
        obj = await get_object_store().get(key)
    except Exception as e:
        logger.warning("Rollup fetch failed", key=key, error=str(e))
        detail = "object store unavailable" if is_retryable(e) else "object store error"
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=detail
        ) from e

    if obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"no rollup for {day}"
        )

    return Response(content=obj.body, media_type="application/json")
```

- [ ] **Step 4: Add total_cents to the response model**

In `api/routes/models.py`, add to `OrderResponse` after `quantity`:

```python
    # Cents, as stored. No formatted variant: one representation from the
    # price catalog through Postgres to here means nothing to convert.
    total_cents: int | None = None
```

- [ ] **Step 5: Add the health probe**

In `api/routes/health.py`, add the probe function after `_check_redis`:

```python
async def _check_s3() -> str:
    from storage.s3.client import get_object_store

    await get_object_store().head_bucket()
    return "ok"
```

and add it to the probe tuple:

```python
    for name, probe in (
        ("postgres", _check_postgres),
        ("redis", _check_redis),
        ("s3", _check_s3),
    ):
```

- [ ] **Step 6: Wire the lifespan and the router**

In `main.py`:

Add to the imports:

```python
from api.routes import health, info, orders, rollups
from core.services.pricing import reset_price_catalog
from storage.s3.client import close_object_store, get_object_store
```

Immediately after `logger.info("Starting application")`, before the consumer
is constructed — the consumer's first message may need the store:

```python
    await get_object_store().open()
    logger.info("S3 object store opened")
```

In the shutdown half, next to `await close_relay()`:

```python
    reset_price_catalog()
    await close_object_store()
```

And register the router beside the others:

```python
app.include_router(rollups.router)
```

- [ ] **Step 7: Run test to verify it passes**

Run: `poetry run pytest tests/integration/test_rollup_routes.py tests/unit/test_main.py -v`
Expected: PASS. If `tests/unit/test_main.py` asserts on the registered routes,
add `/rollups/{day}` to its expected set.

- [ ] **Step 8: Commit**

```bash
make fmt && make lint
git add api/routes/rollups.py api/routes/models.py api/routes/health.py main.py tests/integration/test_rollup_routes.py tests/unit/test_main.py
git commit -m "feat: expose the rollup object and probe S3 in the health check"
```

---

### Task 11: Compose, demo target, and docs

**Files:**
- Modify: `docker-compose.yaml`
- Modify: `Makefile`
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Test: `tests/integration/test_order_s3_roundtrip.py` (create)

**Interfaces:**
- Consumes: everything from Tasks 1-10.
- Produces: `make demo-s3`; a seeded `config/prices.json` in the compose bucket.

- [ ] **Step 1: Write the failing test**

Create `tests/integration/test_order_s3_roundtrip.py`:

```python
import asyncio
import json
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.integration

CATALOG = json.dumps(
    {"currency": "USD", "items": {"widget": {"unit_price_cents": 1999}}}
).encode()


async def test_an_order_is_priced_from_s3_and_lands_in_the_rollup(
    object_store, migrated_db, redis_client, db_session
):
    """
    The whole path: POST -> outbox -> relay -> Redis -> consumer -> S3.

    Mirrors tests/integration/test_order_roundtrip.py, with the two S3
    assertions added.
    """
    from main import app

    await object_store.put("config/prices.json", CATALOG, "application/json")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(
            "/orders",
            json={"order_ref": "s3-1", "item": "widget", "quantity": 3},
        )
        assert created.status_code == 201
        assert created.json()["total_cents"] is None  # not priced yet

        for _ in range(50):
            await asyncio.sleep(0.2)
            fetched = await client.get("/orders/s3-1")
            if fetched.json()["status"] == "confirmed":
                break
        else:
            pytest.fail("order was never confirmed")

    assert fetched.json()["total_cents"] == 5997

    # The order was created moments ago, so its creation date is today's —
    # the handler keys the rollup off created_at, not off the clock.
    key = f"rollups/{datetime.now(UTC).date().isoformat()}.json"
    rollup = await object_store.get(key)
    assert rollup is not None
    document = json.loads(rollup.body)
    assert document["total_cents"] == 5997
    assert [o["order_ref"] for o in document["orders"]] == ["s3-1"]
```

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `poetry run pytest tests/integration/test_order_s3_roundtrip.py -v`
Expected: PASS if Tasks 1-10 are correct. If it fails, the failure is a real
integration defect — fix it before continuing rather than adjusting the test.
Compare against `tests/integration/test_order_roundtrip.py` for how that file
starts the consumer and relay; copy its mechanism exactly.

- [ ] **Step 3: Wire compose**

In `docker-compose.yaml`, add to the `api` service's `environment` block:

```yaml
      S3_ENDPOINT_URL: http://seaweedfs:8333
      S3_REGION: us-east-1
      S3_BUCKET: pmt-bucket
      S3_ACCESS_KEY_ID: dev
      S3_SECRET_ACCESS_KEY: dev
      S3_PRICES_KEY: config/prices.json
      S3_ROLLUP_PREFIX: rollups/
      PRICES_CACHE_TTL_S: 60
```

and to its `depends_on`:

```yaml
      seaweedfs:
        condition: service_healthy
```

Then extend the `bucket-init` command so the bucket is never empty — replace
the two lines after `aws --endpoint-url "$$endpoint" s3api head-bucket ...`
with:

```yaml
        aws --endpoint-url "$$endpoint" s3api head-bucket --bucket "$$bucket"
        # Seed the catalog: without it, the first order dead-letters on a
        # missing price file, which is a confusing first experience.
        if aws --endpoint-url "$$endpoint" s3api head-object --bucket "$$bucket" --key config/prices.json 2>/dev/null; then
          echo "prices.json already present"
        else
          echo '{"currency":"USD","items":{"widget":{"unit_price_cents":1999},"gadget":{"unit_price_cents":4550}}}' \
            | aws --endpoint-url "$$endpoint" s3 cp - "s3://$$bucket/config/prices.json"
        fi
        echo "bucket ready: $$bucket"
```

- [ ] **Step 4: Add the demo target**

In `Makefile`, add `demo-s3` to the `.PHONY` line and append:

```makefile
demo-s3:           ## Watch an order get priced from S3 and land in the rollup
	@echo "--- price catalog in S3"
	@docker compose run --rm --entrypoint sh s3-put -c \
		"aws --endpoint-url http://seaweedfs:8333 s3 cp s3://pmt-bucket/config/prices.json -"
	@echo "--- POST /orders"
	@curl -s -X POST http://localhost:8000/orders \
		-H 'Content-Type: application/json' \
		-d '{"order_ref":"demo-s3-1","item":"widget","quantity":3}' | python3 -m json.tool
	@echo "--- waiting for the consumer..."
	@sleep 3
	@echo "--- GET /orders/demo-s3-1 (total_cents comes from S3)"
	@curl -s http://localhost:8000/orders/demo-s3-1 | python3 -m json.tool
	@echo "--- GET /rollups/$$(date -u +%F) (the object the consumer wrote)"
	@curl -s http://localhost:8000/rollups/$$(date -u +%F) | python3 -m json.tool
```

- [ ] **Step 5: Verify the demo end to end**

Run:

```bash
make down && make up
sleep 15
make demo-s3
```

Expected: the catalog prints, the order comes back `confirmed` with
`"total_cents": 5997`, and the rollup document lists `demo-s3-1`.

- [ ] **Step 6: Document it in CLAUDE.md**

Add a `## Storage` section after the `## Outbox` section:

```markdown
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
```

Also add to the Consumer section's bullet list:

```markdown
- A handler raising `PermanentHandlerError` (`storage/errors.py`) is
  dead-lettered immediately with reason `permanent_handler_error`, no retries —
  the same treatment a `ValidationError` gets, for the same reason.
```

- [ ] **Step 7: Document it in README.md**

Add SeaweedFS to the architecture/services list, document the `S3_*` settings
alongside the existing ones, and add `make demo-s3` to the quickstart beside
`make demo`. Add `storage/` to the "Make it yours" checklist as a layer to
keep or delete.

- [ ] **Step 8: Full verification**

Run:

```bash
make fmt && make lint
poetry run pytest -m "not integration" -q
poetry run pytest -q
```

Expected: lint clean, unit suite green with no container started, full suite
green.

- [ ] **Step 9: Commit**

```bash
git add docker-compose.yaml Makefile README.md CLAUDE.md tests/integration/test_order_s3_roundtrip.py
git commit -m "feat: seed the price catalog in compose and add make demo-s3"
```
