import json
from datetime import UTC, date, datetime
from types import SimpleNamespace

from storage.fake import FakeObjectStore


class _StubRepo:
    """Stands in for OrderRepository; RollupService only calls one method."""

    def __init__(self, orders):
        self._orders = orders

    async def list_confirmed_created_on(self, day):
        return self._orders


def _order(ref, item, qty, total_cents):
    return SimpleNamespace(
        order_ref=ref,
        item=item,
        quantity=qty,
        total_cents=total_cents,
        confirmed_at=datetime(2026, 9, 7, 12, 0, tzinfo=UTC),
    )


def _service(store, orders):
    from core.services.rollup_service import RollupService

    service = RollupService(session=None, store=store)
    service.repo = _StubRepo(orders)
    return service


async def test_rebuild_writes_the_expected_document():
    store = FakeObjectStore()
    orders = [_order("a", "widget", 3, 5997), _order("b", "gadget", 1, 4550)]

    key = await _service(store, orders).rebuild_for_date(date(2026, 9, 7))

    assert key == "rollups/2026-09-07.json"
    document = json.loads(store.puts[key])
    assert document == {
        "date": "2026-09-07",
        "currency": "USD",
        "order_count": 2,
        "total_cents": 10547,
        "orders": [
            {
                "order_ref": "a",
                "item": "widget",
                "quantity": 3,
                "unit_price_cents": 1999,
                "total_cents": 5997,
            },
            {
                "order_ref": "b",
                "item": "gadget",
                "quantity": 1,
                "unit_price_cents": 4550,
                "total_cents": 4550,
            },
        ],
    }


async def test_rebuild_writes_an_empty_document_when_there_are_no_orders():
    store = FakeObjectStore()

    key = await _service(store, []).rebuild_for_date(date(2026, 9, 7))

    document = json.loads(store.puts[key])
    assert document["orders"] == []
    assert document["total_cents"] == 0
    assert document["order_count"] == 0


async def test_rebuild_overwrites_rather_than_merging():
    store = FakeObjectStore({"rollups/2026-09-07.json": b'{"stale": true}'})

    await _service(store, [_order("a", "widget", 1, 1999)]).rebuild_for_date(
        date(2026, 9, 7)
    )

    document = json.loads(store.puts["rollups/2026-09-07.json"])
    assert "stale" not in document
    assert store.get_calls == 0  # the projection never reads the old object


async def test_rebuild_skips_orders_with_no_total():
    store = FakeObjectStore()
    orders = [_order("a", "widget", 1, 1999), _order("b", "gadget", 1, None)]

    await _service(store, orders).rebuild_for_date(date(2026, 9, 7))

    document = json.loads(store.puts["rollups/2026-09-07.json"])
    assert [o["order_ref"] for o in document["orders"]] == ["a"]
