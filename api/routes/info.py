from fastapi import APIRouter, Depends

from config.logging import get_logger
from config.settings import get_settings

logger = get_logger(__name__)


router = APIRouter(prefix="/info", tags=["info"])


@router.get("", summary="Info")
async def info(settings=Depends(get_settings)):
    return {
        "service": settings.SERVICE_NAME,
        "version": settings.SERVICE_VERSION,
        "environment": settings.ENVIRONMENT,
    }
