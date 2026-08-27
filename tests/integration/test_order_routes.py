# tests/integration/test_order_routes.py

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.integration


@pytest.fixture
async def client(app_settings, migrated_db, redis_client):
    from db.postgres.session import get_db
    from main import app

    async def override_get_db():
        from sqlalchemy.ext.asyncio import (
            AsyncSession,
            async_sessionmaker,
            create_async_engine,
        )

        engine = create_async_engine(app_settings.DATABASE_URL)
        maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with maker() as session:
            yield session
        await engine.dispose()

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


async def test_post_creates_pending_order(client, db_session):
    response = await client.post(
        "/orders", json={"order_ref": "r1", "item": "widget", "quantity": 2}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["order_ref"] == "r1"
    assert body["status"] == "pending"
    assert body["confirmed_at"] is None


async def test_get_returns_the_order(client, db_session):
    await client.post(
        "/orders", json={"order_ref": "r2", "item": "widget", "quantity": 1}
    )
    response = await client.get("/orders/r2")
    assert response.status_code == 200
    assert response.json()["item"] == "widget"


async def test_get_unknown_ref_is_404(client, db_session):
    assert (await client.get("/orders/ghost")).status_code == 404


async def test_duplicate_order_ref_is_409(client, db_session):
    payload = {"order_ref": "r3", "item": "widget", "quantity": 1}
    assert (await client.post("/orders", json=payload)).status_code == 201
    assert (await client.post("/orders", json=payload)).status_code == 409


async def test_zero_quantity_is_422(client, db_session):
    response = await client.post(
        "/orders", json={"order_ref": "r4", "item": "widget", "quantity": 0}
    )
    assert response.status_code == 422
