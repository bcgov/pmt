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
