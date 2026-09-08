# tests/unit/test_redis_consumer.py

import pytest
from redis.exceptions import RedisError, ResponseError

from messaging.consumer import redis_consumer as redis_consumer_module
from messaging.consumer.redis_consumer import RedisConsumer
from messaging.models import EventEnvelope, OrderCreatedEvent
from storage.errors import PermanentHandlerError


class FakeRedis:
    """A hand-rolled Redis double: enough surface for RedisConsumer, no I/O."""

    def __init__(self):
        self.xadd_calls = []
        self.xack_calls = []
        self.ensure_group_error = None
        self.xautoclaim_result = ("0-0", [], [])
        self.xautoclaim_error = None
        self.xpending_range_result = []
        self.xreadgroup_impl = None

    async def xgroup_create(self, **kwargs):
        if self.ensure_group_error:
            raise self.ensure_group_error

    async def xadd(self, name, fields):
        self.xadd_calls.append((name, fields))
        return "1-0"

    async def xack(self, stream, group, message_id):
        self.xack_calls.append(message_id)

    async def xautoclaim(self, **kwargs):
        if self.xautoclaim_error:
            raise self.xautoclaim_error
        return self.xautoclaim_result

    async def xpending_range(self, *args, **kwargs):
        return self.xpending_range_result

    async def xreadgroup(self, **kwargs):
        return await self.xreadgroup_impl(**kwargs)


def make_consumer():
    consumer = RedisConsumer(consumer_name="test")
    consumer.redis = FakeRedis()
    return consumer


async def fast_sleep(_):
    return


def valid_envelope_json():
    envelope = EventEnvelope.create(
        event_type="OrderCreated",
        payload=OrderCreatedEvent(order_ref="r1", item="widget", quantity=1),
        correlation_id="corr-1",
        source="test",
    )
    return envelope.model_dump_json()


async def test_ensure_group_reraises_non_busygroup_errors():
    consumer = make_consumer()
    consumer.redis.ensure_group_error = ResponseError("WRONGTYPE bad key")

    with pytest.raises(ResponseError):
        await consumer.ensure_group()


async def test_handle_one_dead_letters_when_event_field_missing():
    consumer = make_consumer()

    await consumer._handle_one("1-0", {})

    stream, fields = consumer.redis.xadd_calls[0]
    assert stream == consumer.dlq_stream
    assert fields["reason"] == "missing_field"
    assert consumer.redis.xack_calls == ["1-0"]


async def test_reclaim_orphans_returns_quietly_on_xautoclaim_error():
    consumer = make_consumer()
    consumer.redis.xautoclaim_error = ResponseError("NOGROUP no such key")

    await consumer.reclaim_orphans()  # must not raise

    assert consumer.redis.xadd_calls == []


async def test_reclaim_orphans_dead_letters_poison_messages():
    consumer = make_consumer()
    consumer.redis.xautoclaim_result = ("0-0", [("5-0", {"event": "stale"})], [])
    consumer.redis.xpending_range_result = [
        {"times_delivered": consumer.max_retries + 1}
    ]
    handled = []

    async def fake_handle_one(message_id, fields):
        handled.append(message_id)

    consumer._handle_one = fake_handle_one

    await consumer.reclaim_orphans()

    assert handled == []
    stream, fields = consumer.redis.xadd_calls[0]
    assert stream == consumer.dlq_stream
    assert fields["reason"] == "poison_message"
    assert consumer.redis.xack_calls == ["5-0"]


async def test_reclaim_orphans_hands_off_messages_under_the_retry_limit():
    consumer = make_consumer()
    consumer.redis.xautoclaim_result = ("0-0", [("6-0", {"event": "e"})], [])
    consumer.redis.xpending_range_result = [{"times_delivered": 1}]
    handled = []

    async def fake_handle_one(message_id, fields):
        handled.append(message_id)

    consumer._handle_one = fake_handle_one

    await consumer.reclaim_orphans()

    assert handled == ["6-0"]
    assert consumer.redis.xadd_calls == []  # not dead-lettered


async def test_start_loop_swallows_response_error_and_keeps_running(monkeypatch):
    monkeypatch.setattr(redis_consumer_module.asyncio, "sleep", fast_sleep)
    consumer = make_consumer()
    calls = {"n": 0}

    async def fake_xreadgroup(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ResponseError("NOGROUP no such key")
        consumer.running = False
        return []

    consumer.redis.xreadgroup_impl = fake_xreadgroup

    await consumer.start()

    assert calls["n"] == 2


async def test_start_loop_swallows_redis_connection_errors(monkeypatch):
    monkeypatch.setattr(redis_consumer_module.asyncio, "sleep", fast_sleep)
    consumer = make_consumer()
    calls = {"n": 0}

    async def fake_xreadgroup(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RedisError("connection reset")
        consumer.running = False
        return []

    consumer.redis.xreadgroup_impl = fake_xreadgroup

    await consumer.start()

    assert calls["n"] == 2


async def test_start_loop_stops_mid_batch_when_stop_is_requested(monkeypatch):
    monkeypatch.setattr(redis_consumer_module.asyncio, "sleep", fast_sleep)
    consumer = make_consumer()
    handled = []

    async def fake_handle_one(message_id, fields):
        handled.append(message_id)
        consumer.running = False  # e.g. stop() called by another task mid-batch

    consumer._handle_one = fake_handle_one

    async def fake_xreadgroup(**kwargs):
        return [("stream", [("1-0", {"event": "a"}), ("2-0", {"event": "b"})])]

    consumer.redis.xreadgroup_impl = fake_xreadgroup

    await consumer.start()

    assert handled == ["1-0"]


async def test_a_permanent_handler_error_is_dead_lettered_without_retries(monkeypatch):
    calls = []

    async def failing_dispatch(envelope):
        calls.append(envelope)
        raise PermanentHandlerError("no price for item: gizmo")

    monkeypatch.setattr(redis_consumer_module, "dispatch_event", failing_dispatch)

    consumer = make_consumer()

    await consumer._handle_one("1-0", {"event": valid_envelope_json()})

    assert len(calls) == 1, "a permanent error must not be retried"
    stream, fields = consumer.redis.xadd_calls[0]
    assert stream == consumer.dlq_stream
    assert fields["reason"] == "permanent_handler_error"
    assert consumer.redis.xack_calls == ["1-0"]


async def test_a_generic_handler_error_still_retries(monkeypatch):
    monkeypatch.setattr(redis_consumer_module.asyncio, "sleep", fast_sleep)
    calls = []

    async def failing_dispatch(envelope):
        calls.append(envelope)
        raise RuntimeError("transient")

    monkeypatch.setattr(redis_consumer_module, "dispatch_event", failing_dispatch)

    consumer = make_consumer()

    await consumer._handle_one("1-0", {"event": valid_envelope_json()})

    assert len(calls) == consumer.max_retries
    stream, fields = consumer.redis.xadd_calls[0]
    assert stream == consumer.dlq_stream
    assert fields["reason"] == "handler_error"
    assert consumer.redis.xack_calls == ["1-0"]
