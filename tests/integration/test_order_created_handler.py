import json

import pytest

from core.services import pricing as pricing_module
from db.repositories.order_repository import OrderRepository
from messaging.consumer.handlers.order_created import handle
from messaging.models import OrderCreatedEvent
from storage.errors import PermanentHandlerError

pytestmark = pytest.mark.integration

CATALOG = json.dumps(
    {"currency": "USD", "items": {"widget": {"unit_price_cents": 1999}}}
).encode()


@pytest.fixture(autouse=True)
async def _wire_object_store(object_store):
    """
    handle() reaches S3 through the process-wide get_object_store()
    singleton, not through injection. Point it at this test's already-opened
    store, so the catalog the handler prices from and the rollup it writes
    land in the same bucket this test seeds and can assert against.
    """
    import storage.s3.client as client_module

    client_module._store = object_store
    pricing_module._catalog = None
    await object_store.put("config/prices.json", CATALOG, "application/json")
    yield
    pricing_module._catalog = None


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
    assert order.total_cents == 1999


async def test_handler_is_idempotent_on_replay(app_settings, db_session):
    repo = OrderRepository(db_session)
    await repo.create(order_ref="r2", item="widget", quantity=1)
    await db_session.commit()

    payload = OrderCreatedEvent(order_ref="r2", item="widget", quantity=1)
    await handle(payload, correlation_id="c")
    await handle(payload, correlation_id="c")  # replay must not raise


async def test_handler_raises_permanent_error_for_a_missing_order(
    app_settings, db_session
):
    with pytest.raises(PermanentHandlerError, match="ghost"):
        await handle(
            OrderCreatedEvent(order_ref="ghost", item="widget", quantity=1),
            correlation_id="c",
        )
