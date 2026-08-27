import pytest

from db.repositories.order_repository import OrderRepository

pytestmark = pytest.mark.integration


async def test_create_then_get(db_session):
    repo = OrderRepository(db_session)
    await repo.create(order_ref="ref-1", item="widget", quantity=3)
    await db_session.commit()

    found = await repo.get_by_ref("ref-1")
    assert found is not None
    assert found.item == "widget"
    assert found.quantity == 3
    assert found.status == "pending"
    assert found.confirmed_at is None


async def test_get_missing_returns_none(db_session):
    repo = OrderRepository(db_session)
    assert await repo.get_by_ref("nope") is None


async def test_confirm_moves_pending_to_confirmed(db_session):
    repo = OrderRepository(db_session)
    await repo.create(order_ref="ref-2", item="widget", quantity=1)
    await db_session.commit()

    assert await repo.confirm("ref-2") is True
    await db_session.commit()

    found = await repo.get_by_ref("ref-2")
    assert found.status == "confirmed"
    assert found.confirmed_at is not None


async def test_confirm_is_idempotent(db_session):
    """Redis Streams is at-least-once; a replay must be a no-op, not an error."""
    repo = OrderRepository(db_session)
    await repo.create(order_ref="ref-3", item="widget", quantity=1)
    await db_session.commit()

    assert await repo.confirm("ref-3") is True
    await db_session.commit()
    assert await repo.confirm("ref-3") is False


async def test_confirm_unknown_ref_returns_false(db_session):
    repo = OrderRepository(db_session)
    assert await repo.confirm("ghost") is False
