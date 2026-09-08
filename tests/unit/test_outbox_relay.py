import asyncio
from datetime import UTC, datetime

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from messaging.outbox.relay import OutboxRelay, close_relay, get_relay


class FakeRow:
    def __init__(self, id: int, payload: str = '{"a": 1}'):
        self.id = id
        self.payload = payload
        self.status = "pending"
        self.attempts = 0
        self.last_error = None
        self.published_at = None
        self.failed_at = None
        self.next_attempt_at = datetime.now(UTC)


class FakeRepo:
    """Mirrors OutboxRepository's contract without a database."""

    def __init__(self, rows):
        self.rows = rows

    async def claim_batch(self, limit):
        return self.rows[:limit]

    async def mark_published(self, row):
        row.status = "published"
        row.published_at = datetime.now(UTC)

    async def mark_failed(self, row, error):
        row.status = "failed"
        row.failed_at = datetime.now(UTC)
        row.last_error = error

    async def mark_retry(self, row, error, backoff_ms):
        row.attempts += 1
        row.last_error = error
        row.backoff_ms = backoff_ms

    async def sweep_published(self, older_than):
        # The loop sweeps on its first iteration; Task 6's tests reuse this.
        return 0


class FakeProducer:
    def __init__(self, raise_on=None, exc=None):
        self.published = []
        self.raise_on = raise_on  # attempt index that fails (0-indexed)
        self.exc = exc
        self.attempt = 0

    async def publish_raw(self, payload):
        if self.raise_on is not None and self.attempt == self.raise_on:
            self.attempt += 1
            raise self.exc
        self.published.append(payload)
        self.attempt += 1
        return "1-0"


class FakeSession:
    def __init__(self):
        self.committed = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def begin(self):
        return _FakeBegin(self)


class _FakeBegin:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, *args):
        if exc_type is None:
            self.session.committed += 1
        return False


def make_relay(rows, producer):
    session = FakeSession()
    relay = OutboxRelay(producer=producer, session_maker=lambda: session)
    relay._repo_factory = lambda s: FakeRepo(rows)
    relay.batch_size = 10
    return relay, session


async def test_drain_publishes_every_row_and_marks_it_published():
    rows = [FakeRow(1), FakeRow(2), FakeRow(3)]
    producer = FakeProducer()
    relay, session = make_relay(rows, producer)

    claimed = await relay.drain_once()

    assert claimed == 3
    assert len(producer.published) == 3
    assert [r.status for r in rows] == ["published"] * 3
    assert session.committed == 1


async def test_transport_failure_keeps_the_row_pending_and_stops_the_batch():
    """
    If Redis is unreachable for one row it is unreachable for all of them.
    Breaking preserves publish order instead of burning attempts on every row.
    """
    rows = [FakeRow(1), FakeRow(2), FakeRow(3)]
    producer = FakeProducer(raise_on=1, exc=RedisConnectionError("down"))
    relay, _ = make_relay(rows, producer)

    await relay.drain_once()

    assert rows[0].status == "published"
    assert rows[1].status == "pending"
    assert rows[1].attempts == 1
    assert rows[1].last_error == "down"
    assert rows[2].status == "pending"
    assert rows[2].attempts == 0  # never attempted


async def test_permanent_failure_marks_the_row_and_continues_the_batch():
    """
    One unpublishable row must not block the rows behind it forever.
    """
    rows = [FakeRow(1), FakeRow(2), FakeRow(3)]
    producer = FakeProducer(raise_on=0, exc=ResponseError("WRONGTYPE"))
    relay, _ = make_relay(rows, producer)

    await relay.drain_once()

    assert rows[0].status == "failed"
    assert rows[0].last_error == "WRONGTYPE"
    assert rows[1].status == "published"
    assert rows[2].status == "published"


async def test_backoff_grows_with_the_attempt_count():
    row = FakeRow(1)
    row.attempts = 2
    producer = FakeProducer(raise_on=0, exc=RedisConnectionError("down"))
    relay, _ = make_relay([row], producer)
    relay.retry_backoff_ms = 500
    relay.max_backoff_ms = 30_000

    await relay.drain_once()

    assert row.backoff_ms == 2000


async def test_empty_payload_is_a_permanent_integrity_failure():
    """
    Unreachable in practice — the writer serializes a validated envelope in
    the same transaction as the order row. It exists so corruption surfaces as
    a failed row instead of a crashed relay.
    """
    row = FakeRow(1, payload="")
    producer = FakeProducer()
    relay, _ = make_relay([row], producer)

    await relay.drain_once()

    assert row.status == "failed"
    assert producer.published == []


async def test_stalled_batch_on_transport_failure_reports_no_progress():
    """
    When Redis is down, the first row's transport failure stops the batch
    with nothing reaching a terminal state. drain_once must report 0 so the
    loop waits instead of immediately re-claiming and busy-spinning.
    """
    rows = [FakeRow(1), FakeRow(2), FakeRow(3)]
    producer = FakeProducer(raise_on=0, exc=RedisConnectionError("down"))
    relay, _ = make_relay(rows, producer)

    processed = await relay.drain_once()

    assert processed == 0
    assert rows[0].status == "pending"
    assert rows[1].status == "pending"
    assert rows[2].status == "pending"


async def test_an_empty_claim_publishes_nothing():
    producer = FakeProducer()
    relay, session = make_relay([], producer)

    assert await relay.drain_once() == 0
    assert producer.published == []


async def test_notify_wakes_the_loop_before_the_poll_interval_elapses():
    """
    The nudge is a latency optimization only: it reaches the relay in this
    process, so the poll interval is still the actual guarantee.
    """
    relay = OutboxRelay(producer=FakeProducer(), session_maker=lambda: FakeSession())
    relay._repo_factory = lambda s: FakeRepo([])
    relay.poll_interval_ms = 60_000  # long enough that only notify() can win

    task = asyncio.create_task(relay.start())
    await asyncio.sleep(0.05)
    drains_before = relay.drain_count

    relay.notify()
    await asyncio.sleep(0.05)

    assert relay.drain_count > drains_before

    await relay.stop()
    await asyncio.wait_for(task, timeout=5)


async def test_stop_ends_the_loop():
    relay = OutboxRelay(producer=FakeProducer(), session_maker=lambda: FakeSession())
    relay._repo_factory = lambda s: FakeRepo([])
    relay.poll_interval_ms = 10

    task = asyncio.create_task(relay.start())
    await asyncio.sleep(0.05)
    await relay.stop()

    await asyncio.wait_for(task, timeout=5)
    assert relay.running is False


async def test_a_drain_error_does_not_kill_the_loop():
    """
    A database blip must not silently end publishing for the process's life.
    """
    relay = OutboxRelay(producer=FakeProducer(), session_maker=lambda: FakeSession())
    calls = {"n": 0}

    async def exploding_drain():
        calls["n"] += 1
        raise RuntimeError("database is gone")

    relay.drain_once = exploding_drain
    relay.poll_interval_ms = 10

    task = asyncio.create_task(relay.start())
    await asyncio.sleep(0.1)
    await relay.stop()
    await asyncio.wait_for(task, timeout=5)

    assert calls["n"] > 1


async def test_get_relay_returns_one_instance_per_process():
    await close_relay()
    assert get_relay() is get_relay()
    await close_relay()


async def test_notify_before_start_is_safe():
    """
    create_order calls notify() unconditionally, including when RELAY_ENABLED
    is false and nothing ever started a loop.
    """
    relay = OutboxRelay(producer=FakeProducer(), session_maker=lambda: FakeSession())
    relay.notify()  # must not raise
