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
    from storage.fake import FakeObjectStore
    from storage.object_store import NOT_MODIFIED

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
