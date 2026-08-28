from opentelemetry import trace
from opentelemetry.propagate import inject
from redis.asyncio import Redis

from config.logging import get_logger
from config.settings import get_settings
from messaging.models import EventEnvelope

logger = get_logger(__name__)
tracer = trace.get_tracer(__name__)


class RedisProducer:
    """
    Redis Streams producer.

    Publishes validated envelopes. No schema logic, no payload construction.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self.redis = Redis.from_url(settings.REDIS_STREAM_URL, decode_responses=True)
        self.stream_name = settings.STREAM_NAME

    async def publish(self, envelope: EventEnvelope) -> str:
        """
        Publish one envelope. Message shape is {"event": "<json>"} — the
        consumer reads the same key.

        Trace context is read from whatever span is CURRENT, not passed in.
        That is deliberate: the same producer is called from the CLI (where the
        current span is the CLI's root) and from inside a handler (where it is
        the span for the message being handled), and both cases chain correctly
        with no argument threading.
        """
        with tracer.start_as_current_span(f"publish {envelope.event_type}"):
            carrier: dict[str, str] = {}
            inject(carrier)
            envelope = envelope.model_copy(
                update={"traceparent": carrier.get("traceparent")}
            )

            try:
                message_id = await self.redis.xadd(
                    name=self.stream_name,
                    fields={"event": envelope.model_dump_json()},
                )
                logger.info(
                    "Event published",
                    event_id=str(envelope.event_id),
                    event_type=envelope.event_type,
                    stream=self.stream_name,
                    message_id=message_id,
                )
                return message_id
            except Exception as e:
                logger.error(
                    "Failed to publish event",
                    event_type=envelope.event_type,
                    error=str(e),
                    exc_info=True,
                )
                raise

    async def close(self) -> None:
        await self.redis.aclose()
        logger.debug("Redis producer connection closed")


_producer: RedisProducer | None = None


def get_producer() -> RedisProducer:
    """
    Process-wide producer. Lazily constructed so importing this module does
    not open a socket.
    """
    global _producer
    if _producer is None:
        _producer = RedisProducer()
    return _producer


async def close_producer() -> None:
    """Dispose of the process-wide producer. Called from the app lifespan."""
    global _producer
    if _producer is not None:
        await _producer.close()
        _producer = None
