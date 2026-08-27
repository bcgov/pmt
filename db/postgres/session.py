# db/postgres/session.py

from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from config.logging import get_logger
from config.settings import get_settings

logger = get_logger(__name__)


# Base class for SQLAlchemy models
class Base(DeclarativeBase):
    """Base class for all database models."""

    pass


# Global engine and session maker
_engine = None
_async_session_maker = None


def get_engine():
    """Get or create the async database engine."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.DATABASE_URL,
            echo=settings.LOG_LEVEL == "DEBUG",  # Log SQL in debug mode
            pool_pre_ping=True,  # Verify connections before using
            pool_size=5,
            max_overflow=10,
        )
        logger.info(
            "Database engine created", database_url=settings.DATABASE_URL.split("@")[-1]
        )
    return _engine


def get_session_maker():
    """Get or create the async session maker."""
    global _async_session_maker
    if _async_session_maker is None:
        engine = get_engine()
        _async_session_maker = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        logger.debug("Database session maker created")
    return _async_session_maker


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency for FastAPI to get database sessions.

    Usage in route:
        @router.get("/items")
        async def get_items(db: AsyncSession = Depends(get_db)):
            ...
    """
    session_maker = get_session_maker()
    async with session_maker() as session:
        try:
            yield session
        finally:
            await session.close()


async def close_db():
    """Close database connections. Call on shutdown."""
    global _engine
    if _engine:
        await _engine.dispose()
        logger.info("Database engine disposed")
