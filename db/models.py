# db/models.py

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, Index, Integer, String, Text, Uuid, text
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

    # Money is stored in cents as an integer — never a float, never a
    # NUMERIC that invites Decimal round-tripping. BIGINT rather than INT
    # because a 32-bit column caps out near $21M.
    total_cents: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

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


class OutboxEvent(Base):
    """
    One event waiting to be published, written in the same transaction as the
    domain row that produced it.

    `payload` is the exact serialized EventEnvelope. It is TEXT rather than
    JSONB deliberately: JSONB stores a parsed form that reorders keys, strips
    whitespace and normalizes numbers, so it cannot give back the bytes the
    writer produced. The relay treats this column as opaque and never parses
    it.
    """

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # The envelope's own id, lifted out so it is queryable and unique.
    event_id: Mapped[UUID] = mapped_column(Uuid, unique=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)

    payload: Mapped[str] = mapped_column(Text, nullable=False)

    # pending | published | failed
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")

    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        # Serves the relay's claim query.
        Index(
            "ix_outbox_pending",
            "next_attempt_at",
            "id",
            postgresql_where=text("status = 'pending'"),
        ),
        # Serves the retention sweep.
        Index(
            "ix_outbox_published",
            "published_at",
            postgresql_where=text("status = 'published'"),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<OutboxEvent(id={self.id}, type={self.event_type}, "
            f"status={self.status})>"
        )
