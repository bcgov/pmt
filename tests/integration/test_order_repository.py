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


async def test_confirm_stores_the_total_in_cents(db_session):
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

    repo = OrderRepository(db_session)
    today = datetime.now(UTC)
    yesterday = today - timedelta(days=1)

    await repo.create(order_ref="lc-1", item="widget", quantity=1)
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

    repo = OrderRepository(db_session)
    await repo.create(order_ref="lc-3", item="widget", quantity=1)
    await db_session.commit()

    rows = await repo.list_confirmed_created_on(datetime.now(UTC).date())

    assert rows == []
