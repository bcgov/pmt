import pytest

from db.repositories.order_repository import OrderRepository
from messaging.consumer.handlers.order_created import handle
from messaging.models import OrderCreatedEvent

pytestmark = pytest.mark.integration


async def test_handler_confirms_a_pending_order(app_settings, db_session):
    repo = OrderRepository(db_session)
    await repo.create(order_ref="r1", item="widget", quantity=1)
    await db_session.commit()

    await handle(
        OrderCreatedEvent(order_ref="r1", item="widget", quantity=1),
        correlation_id="corr-1",
    )

    await db_session.rollback()  # drop this session's snapshot
    order = await repo.get_by_ref("r1")
    assert order.status == "confirmed"
    assert order.confirmed_at is not None


async def test_handler_is_idempotent_on_replay(app_settings, db_session):
    repo = OrderRepository(db_session)
    await repo.create(order_ref="r2", item="widget", quantity=1)
    await db_session.commit()

    payload = OrderCreatedEvent(order_ref="r2", item="widget", quantity=1)
    await handle(payload, correlation_id="c")
    await handle(payload, correlation_id="c")  # replay must not raise


async def test_handler_tolerates_a_missing_order(app_settings, db_session):
    await handle(
        OrderCreatedEvent(order_ref="ghost", item="w", quantity=1),
        correlation_id="c",
    )
