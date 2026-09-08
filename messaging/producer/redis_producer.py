from redis.asyncio import Redis

from config.logging import get_logger
from config.settings import get_settings
from messaging.models import EventEnvelope

logger = get_logger(__name__)


class RedisProducer:
    """
    Redis Streams producer.

    Publishes validated envelopes. No schema logic, no payload construction.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self.redis = Redis.from_url(settings.REDIS_STREAM_URL, decode_responses=True)
        self.stream_name = settings.STREAM_NAME

    async def publish_raw(self, payload: str) -> str:
        """
        Publish an already-serialized envelope.

        The outbox relay uses this: it moves a stored string to Redis without
        ever parsing it, so the bytes the writer produced are the bytes that
        reach the stream. Message shape is {"event": "<json>"} — the consumer
        reads the same key.
        """
        try:
            message_id = await self.redis.xadd(
                name=self.stream_name,
                fields={"event": payload},
            )
            logger.info(
                "Event published",
                stream=self.stream_name,
                message_id=message_id,
            )
            return message_id
        except Exception as e:
            logger.error(
                "Failed to publish event",
                stream=self.stream_name,
                error=str(e),
                exc_info=True,
            )
            raise

    async def publish(self, envelope: EventEnvelope) -> str:
        """Serialize an envelope and publish it. One XADD call site: publish_raw."""
        return await self.publish_raw(envelope.model_dump_json())

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
