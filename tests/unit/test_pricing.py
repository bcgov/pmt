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
