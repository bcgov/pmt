from datetime import UTC, datetime
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from messaging.models.events.order_created import OrderCreatedEvent

# Add your payload types to this union as the service grows.
EventPayload = OrderCreatedEvent

SCHEMA_VERSION = "1.0.0"


class EventEnvelope(BaseModel):
    """
    The single validation boundary between services.

    Everything on the stream is an envelope; handlers only ever see a
    validated payload.
    """

    event_id: UUID
    event_type: Literal["OrderCreated"]
    timestamp: datetime
    schema_version: str = Field(pattern=r"^\d+\.\d+\.\d+$")
    correlation_id: str
    source: str
    payload: EventPayload

    model_config = {"extra": "forbid"}

    @classmethod
    def create(
        cls,
        event_type: str,
        payload: EventPayload,
        correlation_id: str,
        source: str,
    ) -> "EventEnvelope":
        """Build an envelope, filling in id, timestamp and schema version."""
        return cls(
            event_id=uuid4(),
            event_type=event_type,
            timestamp=datetime.now(UTC),
            schema_version=SCHEMA_VERSION,
            correlation_id=correlation_id,
            source=source,
            payload=payload,
        )
