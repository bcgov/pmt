import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.util._once import Once

from messaging.consumer.redis_consumer import RedisConsumer
from messaging.models import EventEnvelope, OrderCreatedEvent
from messaging.producer.redis_producer import RedisProducer, close_producer
from messaging.state import close_state_client

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def span_exporter():
    """
    Attach an in-memory exporter to whatever TracerProvider is live.

    Session-scoped and additive on purpose. trace.get_tracer() hands modules a
    ProxyTracer that caches the real tracer the first time it resolves one, so
    swapping the provider per test would leave every module written at import
    time pointing at the first provider. Hooking the exporter onto the live
    provider sidesteps that entirely.
    """
    provider = trace.get_tracer_provider()
    if not isinstance(provider, TracerProvider):
        # Nothing has installed a real provider yet (init_tracing() has not
        # run in this process). Install one; the set-once guard has to be
        # re-armed by hand because there is no public reset.
        trace._TRACER_PROVIDER = None
        trace._TRACER_PROVIDER_SET_ONCE = Once()
        provider = TracerProvider()
        trace.set_tracer_provider(provider)

    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


@pytest.fixture
def spans(span_exporter):
    """Spans finished during one test, and only that test."""
    span_exporter.clear()
    yield span_exporter
    span_exporter.clear()


@pytest.fixture(autouse=True)
async def _close_clients(app_settings):
    yield
    await close_state_client()
    await close_producer()


def by_name(exporter, name):
    return [s for s in exporter.get_finished_spans() if s.name == name]


async def test_publish_injects_traceparent_into_the_envelope(
    app_settings, redis_client, spans
):
    producer = RedisProducer()
    await producer.publish(
        EventEnvelope.create(
            event_type="OrderCreated",
            payload=OrderCreatedEvent(
                order_ref="r1", item="widget", quantity=3, unit_price_cents=450
            ),
            correlation_id="corr-1",
            source="test",
        )
    )
    await producer.close()

    entries = await redis_client.xrange(app_settings.STREAM_NAME)
    published = EventEnvelope.model_validate_json(entries[0][1]["event"])

    assert published.traceparent is not None
    publish_span = by_name(spans, "publish OrderCreated")[0]
    assert format(publish_span.context.trace_id, "032x") in published.traceparent


async def test_the_full_chain_is_one_trace_across_both_hops(
    app_settings, redis_client, spans
):
    """
    The assertion this whole design exists for: cli publish -> consume
    OrderCreated -> publish OrderConfirmed -> consume OrderConfirmed, all in
    one trace, each span parented to the one before it.
    """
    tracer = trace.get_tracer("test")
    producer = RedisProducer()
    with tracer.start_as_current_span("cli.publish"):
        await producer.publish(
            EventEnvelope.create(
                event_type="OrderCreated",
                payload=OrderCreatedEvent(
                    order_ref="r1", item="widget", quantity=3, unit_price_cents=450
                ),
                correlation_id="corr-1",
                source="cli",
            )
        )

    consumer = RedisConsumer()
    await consumer.ensure_group()

    # Two passes: the first handles OrderCreated (which publishes
    # OrderConfirmed), the second handles OrderConfirmed.
    for _ in range(2):
        response = await consumer.redis.xreadgroup(
            groupname=consumer.consumer_group,
            consumername=consumer.consumer_name,
            streams={consumer.stream_name: ">"},
            count=10,
            block=1000,
        )
        for _stream, messages in response or []:
            for message_id, fields in messages:
                await consumer._handle_one(message_id, fields)

    await consumer.close()

    root = by_name(spans, "cli.publish")[0]
    consume_created = by_name(spans, "consume OrderCreated")[0]
    publish_confirmed = by_name(spans, "publish OrderConfirmed")[0]
    consume_confirmed = by_name(spans, "consume OrderConfirmed")[0]

    trace_id = root.context.trace_id
    assert consume_created.context.trace_id == trace_id
    assert publish_confirmed.context.trace_id == trace_id
    assert consume_confirmed.context.trace_id == trace_id

    assert (
        consume_created.parent.span_id
        == by_name(spans, "publish OrderCreated")[0].context.span_id
    )
    assert publish_confirmed.parent.span_id == consume_created.context.span_id
    assert consume_confirmed.parent.span_id == publish_confirmed.context.span_id


async def test_an_envelope_without_traceparent_starts_a_new_trace(
    app_settings, redis_client, spans
):
    consumer = RedisConsumer()
    await consumer.ensure_group()

    envelope = EventEnvelope.create(
        event_type="OrderCreated",
        payload=OrderCreatedEvent(
            order_ref="r2", item="widget", quantity=1, unit_price_cents=100
        ),
        correlation_id="corr-2",
        source="test",
    )
    assert envelope.traceparent is None

    await consumer._handle_one("1-1", {"event": envelope.model_dump_json()})
    await consumer.close()

    span = by_name(spans, "consume OrderCreated")[0]
    assert span.parent is None
