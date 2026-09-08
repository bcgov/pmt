import asyncio
import time

from pydantic import BaseModel, Field, StrictInt, ValidationError

from config.logging import get_logger
from config.settings import get_settings
from storage.errors import PermanentHandlerError
from storage.object_store import NOT_MODIFIED, ObjectStore

logger = get_logger(__name__)


class PriceItem(BaseModel):
    """One priced item. StrictInt so 19.99 fails here, not silently truncates."""

    unit_price_cents: StrictInt = Field(ge=0)


class PriceCatalogDocument(BaseModel):
    """The parsed contents of config/prices.json."""

    currency: str
    items: dict[str, PriceItem]

    def unit_price_cents(self, item: str) -> int:
        entry = self.items.get(item)
        if entry is None:
            raise PermanentHandlerError(f"no price for item: {item}")
        return entry.unit_price_cents


class PriceCatalog:
    """
    The price catalog, cached in process and revalidated with its ETag.

    Config in an object store is read far more often than it changes, so a
    fresh GET per message is waste. After the TTL the next caller sends a
    conditional GET: a 304 costs a round trip and no parse, a 200 replaces
    the cached document. A failed revalidation propagates without discarding
    what is already held — a file that becomes briefly unreachable should not
    invalidate a copy that is almost certainly still correct.
    """

    def __init__(
        self,
        store: ObjectStore,
        *,
        ttl_s: int | None = None,
        key: str | None = None,
    ) -> None:
        settings = get_settings()
        self._store = store
        self._ttl_s = settings.PRICES_CACHE_TTL_S if ttl_s is None else ttl_s
        self._key = key or settings.S3_PRICES_KEY
        self._document: PriceCatalogDocument | None = None
        self._etag: str | None = None
        self._loaded_at = 0.0
        self._lock = asyncio.Lock()

    async def get(self) -> PriceCatalogDocument:
        """The catalog, from cache when fresh, revalidated when not."""
        if self._is_fresh():
            return self._document

        async with self._lock:
            # A caller that waited on the lock may find the work already done.
            if self._is_fresh():
                return self._document
            await self._refresh()
            return self._document

    def _is_fresh(self) -> bool:
        return (
            self._document is not None
            and (time.monotonic() - self._loaded_at) < self._ttl_s
        )

    async def _refresh(self) -> None:
        if self._etag is not None:
            result = await self._store.get_if_none_match(self._key, self._etag)
            if result is NOT_MODIFIED:
                self._loaded_at = time.monotonic()
                logger.debug("Price catalog unchanged", key=self._key)
                return
        else:
            result = await self._store.get(self._key)

        if result is None:
            raise PermanentHandlerError(f"price catalog not found: {self._key}")

        self._document = self._parse(result.body)
        self._etag = result.etag
        self._loaded_at = time.monotonic()
        logger.info(
            "Price catalog loaded",
            key=self._key,
            items=len(self._document.items),
            etag=self._etag,
        )

    def _parse(self, body: bytes) -> PriceCatalogDocument:
        try:
            return PriceCatalogDocument.model_validate_json(body)
        except ValidationError as e:
            # A malformed catalog fails identically on every retry.
            raise PermanentHandlerError(
                f"price catalog at {self._key} is invalid: {e}"
            ) from e


_catalog: PriceCatalog | None = None


def get_price_catalog() -> PriceCatalog:
    """
    Process-wide catalog, so the cache is shared across messages.

    Bound to the process-wide object store, which the lifespan opens.
    """
    global _catalog
    if _catalog is None:
        from storage.s3.client import get_object_store

        _catalog = PriceCatalog(get_object_store())
    return _catalog


def reset_price_catalog() -> None:
    """Drop the cached catalog. Called from the app lifespan on shutdown."""
    global _catalog
    _catalog = None
