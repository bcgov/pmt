from datetime import datetime

from pydantic import BaseModel, Field


class OrderConfirmedEvent(BaseModel):
    """
    Payload for the OrderConfirmed event — the *output* of the processing step.

    total_cents is integer minor units and is computed by the OrderCreated
    handler; it is a value that did not exist on the inbound event.
    """

    order_ref: str = Field(min_length=1, max_length=255)
    total_cents: int = Field(ge=0)
    confirmed_at: datetime

    model_config = {"extra": "forbid"}
