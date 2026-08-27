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


class FakeSession:
    def __init__(self, fail_commit=False):
        self.committed = False
        self.rolled_back = False
        self.fail_commit = fail_commit

    async def commit(self):
        if self.fail_commit:
            raise IntegrityError("INSERT", {}, Exception("duplicate key"))
        self.committed = True

    async def rollback(self):
        self.rolled_back = True


class FakeProducer:
    def __init__(self, fail=False):
        self.fail = fail
        self.published = []

    async def publish(self, envelope):
        if self.fail:
            raise ConnectionError("redis is down")
        self.published.append(envelope)
        return "1-0"


def make_service(session=None, repo=None, producer=None):
    service = OrderService(
        session or FakeSession(), producer=producer or FakeProducer()
    )
    service.repo = repo or FakeRepo()
    return service


async def test_create_order_commits_then_publishes():
    session, producer = FakeSession(), FakeProducer()
    service = make_service(session=session, producer=producer)

    order, message_id = await service.create_order("r1", "widget", 2)

    assert session.committed is True
    assert message_id == "1-0"
    assert len(producer.published) == 1
    assert producer.published[0].payload.order_ref == "r1"
    assert order.status == "pending"


async def test_publish_failure_keeps_the_committed_row_and_returns_no_id():
    """
    The commit and the XADD are not atomic. On publish failure the row
    exists and the event does not, so the caller reports 201 + pending.
    """
    session, producer = FakeSession(), FakeProducer(fail=True)
    service = make_service(session=session, producer=producer)

    order, message_id = await service.create_order("r1", "widget", 2)

    assert session.committed is True
    assert session.rolled_back is False
    assert message_id is None
    assert order.status == "pending"


async def test_duplicate_order_ref_is_rejected_before_any_write():
    existing = Order(order_ref="r1", item="widget", quantity=1, status="pending")
    session, producer = FakeSession(), FakeProducer()
    service = make_service(
        session=session, repo=FakeRepo(existing=existing), producer=producer
    )

    with pytest.raises(DuplicateOrderError):
        await service.create_order("r1", "widget", 2)

    assert session.committed is False
    assert producer.published == []


async def test_concurrent_duplicate_is_caught_by_the_unique_constraint():
    """
    The get_by_ref check and the insert are not atomic: two concurrent
    requests can both pass the check, so the unique constraint is the real
    guard. Its IntegrityError must map onto the same DuplicateOrderError.
    """
    session, producer = FakeSession(fail_commit=True), FakeProducer()
    service = make_service(session=session, producer=producer)

    with pytest.raises(DuplicateOrderError):
        await service.create_order("r1", "widget", 2)

    assert session.rolled_back is True
    assert producer.published == []


async def test_get_order_delegates_to_the_repository():
    existing = Order(order_ref="r1", item="widget", quantity=1, status="pending")
    service = make_service(repo=FakeRepo(existing=existing))

    assert await service.get_order("r1") is existing
