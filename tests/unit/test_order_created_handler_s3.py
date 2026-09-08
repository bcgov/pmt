import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from storage.errors import PermanentHandlerError
from storage.fake import FakeObjectStore

CATALOG = json.dumps(
    {"currency": "USD", "items": {"widget": {"unit_price_cents": 1999}}}
).encode()


class _StubRepo:
    def __init__(self, *, confirmed: bool, orders=None):
        self._confirmed = confirmed
        self._orders = orders or []
        self.confirm_calls = []
        self.order = SimpleNamespace(
            order_ref="h-1",
            item="widget",
            quantity=3,
            total_cents=5997,
            created_at=datetime(2026, 9, 7, 23, 59, tzinfo=UTC),
            confirmed_at=datetime(2026, 9, 8, 0, 1, tzinfo=UTC),
        )

    async def confirm(self, order_ref, total_cents=None):
        self.confirm_calls.append((order_ref, total_cents))
        return self._confirmed

    async def get_by_ref(self, order_ref):
        return self.order

    async def list_confirmed_created_on(self, day):
        return self._orders


@pytest.fixture
def wired(monkeypatch):
    """
    Replace the handler's three collaborators: the store, the session, and
    the repository. Returns the store and repo so tests can assert on them.
    """
    from core.services import pricing
    from core.services import rollup_service as rollup_service_module
    from messaging.consumer.handlers import order_created as module

    store = FakeObjectStore({"config/prices.json": CATALOG})
    repo = _StubRepo(confirmed=True)

    pricing._catalog = pricing.PriceCatalog(store, ttl_s=60, key="config/prices.json")
    monkeypatch.setattr(module, "get_object_store", lambda: store)
    monkeypatch.setattr(module, "OrderRepository", lambda session: repo)
    # RollupService builds its own repository from its own module's import,
    # so patching the handler's name alone would leave a real repository
    # talking to the stub session.
    monkeypatch.setattr(rollup_service_module, "OrderRepository", lambda session: repo)

    class _Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def commit(self):
            return None

    monkeypatch.setattr(module, "get_session_maker", lambda: (lambda: _Session()))
    yield SimpleNamespace(store=store, repo=repo, module=module)
    pricing._catalog = None


def _payload(item="widget", quantity=3):
    from messaging.models import OrderCreatedEvent

    return OrderCreatedEvent(order_ref="h-1", item=item, quantity=quantity)


async def test_handler_prices_the_order_and_confirms_it(wired):
    await wired.module.handle(_payload(), correlation_id="h-1")

    assert wired.repo.confirm_calls == [("h-1", 5997)]


async def test_handler_writes_the_rollup(wired):
    wired.repo._orders = [wired.repo.order]

    await wired.module.handle(_payload(), correlation_id="h-1")

    assert (
        json.loads(wired.store.puts["rollups/2026-09-07.json"])["total_cents"] == 5997
    )


async def test_the_rollup_day_comes_from_the_order_not_the_clock(wired):
    """The stub order was created 2026-09-07 23:59 and confirmed after midnight."""
    wired.repo._orders = [wired.repo.order]

    await wired.module.handle(_payload(), correlation_id="h-1")

    assert "rollups/2026-09-07.json" in wired.store.puts
    assert "rollups/2026-09-08.json" not in wired.store.puts


async def test_a_vanished_order_is_permanent(wired):
    wired.repo.order = None

    with pytest.raises(PermanentHandlerError, match="no such order"):
        await wired.module.handle(_payload(), correlation_id="h-1")


async def test_a_redelivery_still_rebuilds_the_rollup(wired):
    # confirm() returns False: the row was already confirmed by an earlier
    # delivery. The rollup must still be written, or a PUT that failed on
    # that earlier delivery would never be retried.
    wired.repo._confirmed = False

    await wired.module.handle(_payload(), correlation_id="h-1")

    assert wired.store.puts, "the rollup must be rebuilt on redelivery too"


async def test_an_unpriced_item_raises_a_permanent_error(wired):
    with pytest.raises(PermanentHandlerError, match="gizmo"):
        await wired.module.handle(_payload(item="gizmo"), correlation_id="h-1")

    assert wired.repo.confirm_calls == []


async def test_a_store_failure_propagates_for_the_consumer_to_classify(wired):
    wired.store.fail_next(RuntimeError("s3 unreachable"))

    with pytest.raises(RuntimeError):
        await wired.module.handle(_payload(), correlation_id="h-1")
