import json

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _wire_object_store(object_store):
    """
    The rollup route and the health probe reach S3 through the process-wide
    get_object_store() singleton, not through injection. Point it at this
    test's already-opened store, the same way test_order_roundtrip.py does.
    """
    import storage.s3.client as client_module

    client_module._store = object_store
    yield


async def test_get_rollup_returns_the_object(object_store, migrated_db):
    from main import app

    document = {
        "date": "2026-09-07",
        "currency": "USD",
        "order_count": 0,
        "total_cents": 0,
        "orders": [],
    }
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
