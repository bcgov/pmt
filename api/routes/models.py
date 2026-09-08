# api/routes/models.py

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class CreateOrderRequest(BaseModel):
    """Body for POST /orders."""

    order_ref: str = Field(min_length=1, max_length=255)
    item: str = Field(min_length=1, max_length=255)
    quantity: int = Field(gt=0)


class OrderResponse(BaseModel):
    """An order as returned by the API."""

    model_config = ConfigDict(from_attributes=True)

    order_ref: str
    item: str
    quantity: int
    # Cents, as stored. No formatted variant: one representation from the
    # price catalog through Postgres to here means nothing to convert.
    total_cents: int | None = None
    status: str
    confirmed_at: datetime | None
    created_at: datetime
