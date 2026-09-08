"""add total_cents to orders

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("orders", sa.Column("total_cents", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("orders", "total_cents")
