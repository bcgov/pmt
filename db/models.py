# db/models.py

from datetime import UTC, datetime

from sqlalchemy import DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from db.postgres.session import Base


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Order(Base):
    """
    The one demo entity.

    Lifecycle: created by the API as `pending`, moved to `confirmed` by the
    Redis Streams consumer. Replace this file with your own domain.
    """

    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Client-supplied idempotency key.
    order_ref: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, index=True
    )
    item: Mapped[str] = mapped_column(String(255), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)

    # pending | confirmed
    status: Mapped[str] = mapped_column(
        String(50), nullable=False, default="pending", index=True
    )
    confirmed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )

    __table_args__ = (Index("ix_orders_status_created", "status", "created_at"),)

    def __repr__(self) -> str:
        return f"<Order(order_ref={self.order_ref}, status={self.status})>"
