import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config.logging import get_logger
from config.settings import get_settings
from storage.object_store import NOT_MODIFIED, NotModified, ObjectData

logger = get_logger(__name__)

# Codes SeaweedFS and S3 use for "no such object". A conditional GET that
# 304s also arrives as a ClientError, which is why these are matched by code.
_MISSING_CODES = {"NoSuchKey", "404", "NoSuchBucket"}
_NOT_MODIFIED_CODES = {"304", "NotModified"}


class S3ObjectStore:
    """
    ObjectStore backed by any S3-compatible endpoint (SeaweedFS here).

    The client is opened once and held for the process's life, the way the
    Redis producer holds its connection. botocore's internal retries are
    disabled: retry policy for this service lives in the consumer, and two
    layers of it would multiply into a much longer stall than either intends.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self.bucket = settings.S3_BUCKET
        self._session = aioboto3.Session(
            aws_access_key_id=settings.S3_ACCESS_KEY_ID,
            aws_secret_access_key=settings.S3_SECRET_ACCESS_KEY,
            region_name=settings.S3_REGION,
        )
        self._config = Config(
            connect_timeout=settings.S3_CONNECT_TIMEOUT_S,
            read_timeout=settings.S3_READ_TIMEOUT_S,
            retries={"max_attempts": 1, "mode": "standard"},
            signature_version="s3v4",
        )
        self._endpoint_url = settings.S3_ENDPOINT_URL
        self._context = None
        self._client = None

    async def open(self) -> None:
        """Enter the client context. Called from the application lifespan."""
        if self._client is not None:
            return
        self._context = self._session.client(
            "s3", endpoint_url=self._endpoint_url, config=self._config
        )
        self._client = await self._context.__aenter__()
        logger.info("S3 client opened", endpoint=self._endpoint_url, bucket=self.bucket)

    async def close(self) -> None:
        if self._context is not None:
            await self._context.__aexit__(None, None, None)
        self._context = None
        self._client = None
        logger.debug("S3 client closed")

    async def ensure_bucket(self) -> None:
        """Create the bucket if absent. For tests and local bootstrapping."""
        try:
            await self._client.head_bucket(Bucket=self.bucket)
        except ClientError:
            await self._client.create_bucket(Bucket=self.bucket)

    async def get(self, key: str) -> ObjectData | None:
        try:
            response = await self._client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code", "") in _MISSING_CODES:
                return None
            raise
        body = await response["Body"].read()
        return ObjectData(body=body, etag=response["ETag"])

    async def get_if_none_match(
        self, key: str, etag: str
    ) -> ObjectData | NotModified | None:
        try:
            response = await self._client.get_object(
                Bucket=self.bucket, Key=key, IfNoneMatch=etag
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            # botocore raises on a 304; it is a successful answer, not an error.
            if code in _NOT_MODIFIED_CODES or status == 304:
                return NOT_MODIFIED
            if code in _MISSING_CODES:
                return None
            raise
        body = await response["Body"].read()
        return ObjectData(body=body, etag=response["ETag"])

    async def put(self, key: str, body: bytes, content_type: str) -> None:
        await self._client.put_object(
            Bucket=self.bucket, Key=key, Body=body, ContentType=content_type
        )
        logger.debug("Object written", key=key, bytes=len(body))

    async def head_bucket(self) -> None:
        await self._client.head_bucket(Bucket=self.bucket)


_store: S3ObjectStore | None = None


def get_object_store() -> S3ObjectStore:
    """
    Process-wide store. Lazily constructed so importing this module does not
    open a socket — the same contract as get_producer().

    The returned store is unusable until open() has run; the lifespan does
    that before the consumer starts.
    """
    global _store
    if _store is None:
        _store = S3ObjectStore()
    return _store


async def close_object_store() -> None:
    """Dispose of the process-wide store. Called from the app lifespan."""
    global _store
    if _store is not None:
        await _store.close()
        _store = None
