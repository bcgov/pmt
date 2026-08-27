# tests/integration/test_migrations.py

import pytest
from alembic import command
from alembic.config import Config

pytestmark = pytest.mark.integration


def test_upgrade_downgrade_upgrade_is_clean(app_settings):
    """
    Every revision must implement downgrade(). Round-tripping catches the
    empty downgrade() body that hand-edited revisions always eventually have.
    """
    cfg = Config("alembic.ini")
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "head")


def test_head_matches_models(app_settings, migrated_db):
    """
    Migrations must fully reproduce what db/models.py declares — a drifted
    revision (e.g. a unique index expressed as two objects instead of one)
    passes `upgrade head` but leaves a schema alembic itself flags as stale.
    """
    cfg = Config("alembic.ini")
    command.check(cfg)


async def test_confirmed_at_column_exists(db_session):
    """The second revision adds confirmed_at; head must have it."""
    from sqlalchemy import text

    result = await db_session.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'orders' AND column_name = 'confirmed_at'"
        )
    )
    assert result.scalar_one_or_none() == "confirmed_at"
