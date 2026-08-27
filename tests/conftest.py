# tests/conftest.py

import os
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer


@pytest.fixture(scope="session")
def postgres_url() -> str:
    """Start a throwaway Postgres and yield an asyncpg DSN."""
    with PostgresContainer("postgres:18") as pg:
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        url = (
            f"postgresql+asyncpg://{pg.username}:{pg.password}"
            f"@{host}:{port}/{pg.dbname}"
        )
        yield url


@pytest.fixture(scope="session")
def redis_url() -> str:
    """Start a throwaway Redis and yield its URL."""
    with RedisContainer("redis:7") as rc:
        host = rc.get_container_host_ip()
        port = rc.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture(scope="session")
def app_settings(postgres_url: str, redis_url: str):
    """
    Point the application's cached settings at the containers.

    get_settings() is lru_cached, so the env must be set and the cache
    cleared before anything imports a session maker or Redis client.
    """
    from config.settings import get_settings

    os.environ["DATABASE_URL"] = postgres_url
    os.environ["REDIS_STREAM_URL"] = redis_url
    os.environ["STREAM_NAME"] = "test.order.events"
    os.environ["CONSUMER_GROUP"] = "test_group"
    os.environ["CONSUMER_RETRY_BACKOFF_MS"] = "10"
    get_settings.cache_clear()
    yield get_settings()
    get_settings.cache_clear()


@pytest.fixture(scope="session")
def reset_engine_globals(app_settings):
    """
    db.postgres.session caches its engine and sessionmaker in module globals.
    Whichever URL is seen first wins for the whole session, so clear them once
    the container settings are installed. The consumer handler and the health
    probes both go through these globals.
    """
    import db.postgres.session as session_module

    session_module._engine = None
    session_module._async_session_maker = None
    yield
    session_module._engine = None
    session_module._async_session_maker = None


@pytest.fixture(scope="session")
def migrated_db(app_settings, reset_engine_globals) -> None:
    """Create the schema the only supported way: Alembic."""
    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    command.upgrade(cfg, "head")


@pytest_asyncio.fixture(autouse=True)
async def _reset_session_maker_globals():
    """
    db.postgres.session's cached engine is bound to the event loop it was
    created in, but pytest-asyncio gives each test function its own loop.
    Anything that goes through get_session_maker() (consumer handlers,
    health probes) must get a fresh engine per test, or the second test to
    touch it fails with "Event loop is closed".
    """
    yield
    import db.postgres.session as session_module

    if session_module._engine is not None:
        await session_module._engine.dispose()
    session_module._engine = None
    session_module._async_session_maker = None


@pytest_asyncio.fixture
async def db_session(migrated_db) -> AsyncGenerator[AsyncSession, None]:
    """
    A committed session. Cleanup is TRUNCATE, not rollback, because the
    consumer runs on its own connection and cannot see an open transaction.
    """
    from config.settings import get_settings

    engine = create_async_engine(get_settings().DATABASE_URL)
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as session:
        yield session
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE orders RESTART IDENTITY CASCADE"))
    await engine.dispose()


@pytest_asyncio.fixture
async def redis_client(app_settings):
    """Redis client with the stream and DLQ cleared before and after."""
    from redis.asyncio import Redis

    client = Redis.from_url(app_settings.REDIS_STREAM_URL, decode_responses=True)
    await client.delete(app_settings.STREAM_NAME, app_settings.dlq_stream)
    yield client
    await client.delete(app_settings.STREAM_NAME, app_settings.dlq_stream)
    await client.aclose()
