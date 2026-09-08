from enum import Enum, auto
from typing import NamedTuple, Protocol


class ObjectData(NamedTuple):
    """One object's bytes and the ETag the store reported for them."""

    body: bytes
    etag: str


class NotModified(Enum):
    """
    Single-member enum so the sentinel has a type a checker can narrow.

    `ObjectData | NotModified | None` is three distinct outcomes — changed,
    unchanged, gone — and a bare object() would collapse into `Any`.
    """

    token = auto()


NOT_MODIFIED = NotModified.token


class ObjectStore(Protocol):
    """
    The object operations this service needs. Four methods, deliberately.

    Callers depend on this, not on aioboto3, which is what lets every unit
    test run against an in-memory fake with no Docker.
    """

    async def get(self, key: str) -> ObjectData | None:
        """The object, or None when the key does not exist."""
        ...

    async def get_if_none_match(
        self, key: str, etag: str
    ) -> ObjectData | NotModified | None:
        """NOT_MODIFIED when the ETag still matches; the object when it does not."""
        ...

    async def put(self, key: str, body: bytes, content_type: str) -> None:
        """Write an object, overwriting whatever was there."""
        ...

    async def head_bucket(self) -> None:
        """Raise if the bucket is unreachable. Used by the health probe."""
        ...
