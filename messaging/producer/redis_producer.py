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

    async def publish(self, envelope: EventEnvelope) -> str:
        """
        Publish one envelope. Message shape is {"event": "<json>"} — the
        consumer reads the same key.
        """
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
