import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.integration


async def test_health_reports_both_dependencies(app_settings, migrated_db):
    from main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["services"] == {"postgres": "ok", "redis": "ok"}
