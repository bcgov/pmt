from datetime import date

from fastapi import APIRouter, HTTPException, Response, status

from config.logging import get_logger
from config.settings import get_settings
from storage.errors import is_retryable
from storage.s3.client import get_object_store

logger = get_logger(__name__)

router = APIRouter(prefix="/rollups", tags=["rollups"])


@router.get("/{day}", summary="The daily order rollup, straight from S3")
async def get_rollup(day: date) -> Response:
    """
    Stream one day's rollup object.

    The body is returned verbatim rather than parsed and re-serialized: the
    object is the artifact, and re-encoding it would hide a malformed write
    instead of surfacing it. FastAPI's `date` conversion gives a 422 for a
    malformed path segment for free.
    """
    key = get_settings().rollup_key(day)
    try:
        obj = await get_object_store().get(key)
    except Exception as e:
        logger.warning("Rollup fetch failed", key=key, error=str(e))
        detail = "object store unavailable" if is_retryable(e) else "object store error"
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=detail
        ) from e

    if obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"no rollup for {day}"
        )

    return Response(content=obj.body, media_type="application/json")
