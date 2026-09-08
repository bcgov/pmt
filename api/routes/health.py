import asyncio

from fastapi import APIRouter, Response, status
from redis.asyncio import Redis
from sqlalchemy import text

from config.logging import get_logger
from config.settings import get_settings
from db.postgres.session import get_session_maker

logger = get_logger(__name__)

router = APIRouter(prefix="/health", tags=["health"])

PROBE_TIMEOUT_SECONDS = 2.0


async def _check_postgres() -> str:
    session_maker = get_session_maker()
    async with session_maker() as session:
        await session.execute(text("SELECT 1"))
    return "ok"


async def _check_redis() -> str:
    client = Redis.from_url(get_settings().REDIS_STREAM_URL, decode_responses=True)
    try:
        await client.ping()
        return "ok"
    finally:
        await client.aclose()


async def _check_s3() -> str:
    from storage.s3.client import get_object_store

    await get_object_store().head_bucket()
    return "ok"


@router.get("", summary="Health check")
async def health_check(response: Response):
    """
    Probe every dependency and return 503 if any is down.

    A health endpoint that always returns 200 teaches the wrong reflex: it
    tells your orchestrator the service is fine while it cannot reach its
    database.
    """
    details: dict[str, str] = {}

    for name, probe in (
        ("postgres", _check_postgres),
        ("redis", _check_redis),
        ("s3", _check_s3),
    ):
        try:
            details[name] = await asyncio.wait_for(
                probe(), timeout=PROBE_TIMEOUT_SECONDS
            )
        except Exception as e:
            logger.warning("Health probe failed", dependency=name, error=str(e))
            details[name] = "error"

    healthy = all(v == "ok" for v in details.values())
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {"status": "ok" if healthy else "degraded", "services": details}
