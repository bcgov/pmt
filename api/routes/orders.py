# api/routes/orders.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from api.routes.models import CreateOrderRequest, OrderResponse
from config.logging import get_logger
from core.services import DuplicateOrderError, OrderService
from db.postgres.session import get_db

logger = get_logger(__name__)

router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    request: CreateOrderRequest, db: AsyncSession = Depends(get_db)
) -> OrderResponse:
    """
    Persist an order as `pending` and queue OrderCreated in the outbox.

    Both rows commit together, so a 201 means the event will be published.
    The status is `pending` until the consumer confirms it.
    """
    service = OrderService(db)
    try:
        order = await service.create_order(
            order_ref=request.order_ref,
            item=request.item,
            quantity=request.quantity,
        )
    except DuplicateOrderError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e

    return OrderResponse.model_validate(order)


@router.get("/{order_ref}", response_model=OrderResponse)
async def get_order(
    order_ref: str, db: AsyncSession = Depends(get_db)
) -> OrderResponse:
    """Fetch one order. Poll this to watch pending become confirmed."""
    order = await OrderService(db).get_order(order_ref)
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"no such order: {order_ref}"
        )
    return OrderResponse.model_validate(order)
