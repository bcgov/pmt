import pytest
from sqlalchemy.exc import IntegrityError

from core.services.order_service import DuplicateOrderError, OrderService
from db.models import Order


class FakeRepo:
    def __init__(self, existing=None):
        self.existing = existing
        self.created = None

    async def get_by_ref(self, order_ref):
        return self.existing

    async def create(self, order_ref, item, quantity):
        self.created = Order(
            order_ref=order_ref, item=item, quantity=quantity, status="pending"
        )
        return self.created


class FakeOutbox:
    def __init__(self):
        self.added = []

    async def add(self, envelope):
        self.added.append(envelope)
        return envelope


class FakeSession:
    def __init__(self, fail_commit=False):
        self.commits = 0
        self.rolled_back = False
        self.fail_commit = fail_commit

    @property
    def committed(self) -> bool:
        return self.commits > 0

    async def commit(self):
        if self.fail_commit:
            raise IntegrityError("INSERT", {}, Exception("duplicate key"))
        self.commits += 1

    async def rollback(self):
        self.rolled_back = True


class FakeRelay:
    def __init__(self):
        self.notified = 0

    def notify(self):
        self.notified += 1


@pytest.fixture
def relay(monkeypatch):
    """Replace the process-wide relay so notify() is observable."""
    fake = FakeRelay()
    monkeypatch.setattr("core.services.order_service.get_relay", lambda: fake)
    return fake


@pytest.fixture(autouse=True)
def no_redis_on_the_request_path(monkeypatch):
    """
    create_order must not touch Redis at all. OrderService no longer takes a
    producer, so guard the module-level accessor instead: any call fails.
    """

    def explode():
        raise AssertionError("create_order must not touch Redis")

    monkeypatch.setattr("messaging.producer.redis_producer.get_producer", explode)


def make_service(session=None, repo=None, outbox=None):
    service = OrderService(session or FakeSession())
    service.repo = repo or FakeRepo()
    service.outbox = outbox or FakeOutbox()
    return service


async def test_create_order_writes_both_rows_and_commits_exactly_once(relay):
    session, outbox = FakeSession(), FakeOutbox()
    service = make_service(session=session, outbox=outbox)

    order = await service.create_order("r1", "widget", 2)

    assert session.commits == 1
    assert order.status == "pending"
    assert len(outbox.added) == 1
    envelope = outbox.added[0]
    assert envelope.event_type == "OrderCreated"
    assert envelope.payload.order_ref == "r1"
    assert envelope.correlation_id == "r1"
    assert envelope.source == "api"


async def test_create_order_nudges_the_relay_after_committing(relay):
    service = make_service()

    await service.create_order("r1", "widget", 2)

    assert relay.notified == 1


async def test_duplicate_order_ref_is_rejected_before_any_write(relay):
    existing = Order(order_ref="r1", item="widget", quantity=1, status="pending")
    session, outbox = FakeSession(), FakeOutbox()
    service = make_service(
        session=session, repo=FakeRepo(existing=existing), outbox=outbox
    )

    with pytest.raises(DuplicateOrderError):
        await service.create_order("r1", "widget", 2)

    assert session.commits == 0
    assert outbox.added == []
    assert relay.notified == 0


async def test_concurrent_duplicate_is_caught_by_the_unique_constraint(relay):
    """
    The get_by_ref check and the insert are not atomic: two concurrent
    requests can both pass the check, so the unique constraint is the real
    guard. Its IntegrityError must map onto the same DuplicateOrderError, and
    the outbox row rolls back with the order row.
    """
    session = FakeSession(fail_commit=True)
    service = make_service(session=session)

    with pytest.raises(DuplicateOrderError):
        await service.create_order("r1", "widget", 2)

    assert session.rolled_back is True
    assert relay.notified == 0


async def test_get_order_delegates_to_the_repository(relay):
    existing = Order(order_ref="r1", item="widget", quantity=1, status="pending")
    service = make_service(repo=FakeRepo(existing=existing))

    assert await service.get_order("r1") is existing
