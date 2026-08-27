from pydantic import BaseModel, Field


class OrderCreatedEvent(BaseModel):
    """Payload for the OrderCreated event."""

    order_ref: str = Field(min_length=1, max_length=255)
    item: str = Field(min_length=1, max_length=255)
    quantity: int = Field(gt=0)

    model_config = {"extra": "forbid"}
