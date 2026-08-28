from pydantic import BaseModel, Field


class OrderCreatedEvent(BaseModel):
    """
    Payload for the OrderCreated event — the *inputs* to the processing step.

    unit_price_cents is integer minor units: 450 means 4.50. See money.py.
    """

    order_ref: str = Field(min_length=1, max_length=255)
    item: str = Field(min_length=1, max_length=255)
    quantity: int = Field(gt=0)
    unit_price_cents: int = Field(ge=0)

    model_config = {"extra": "forbid"}
