import hashlib

from storage.object_store import NOT_MODIFIED, NotModified, ObjectData


def _etag(body: bytes) -> str:
    """S3 quotes its ETags, and code that strips quotes should be exercised."""
    return f'"{hashlib.md5(body).hexdigest()}"'


class FakeObjectStore:
    """
    In-memory ObjectStore for unit tests.

    Records enough to assert on: how many reads reached the "network"
    (`get_calls`), what was written (`puts`), and an injectable failure
    (`fail_next`) so both branches of the error classifier can be driven
    without a container.
    """

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self._objects: dict[str, bytes] = dict(objects or {})
        self.get_calls = 0
        self.puts: dict[str, bytes] = {}
        self._next_error: Exception | None = None

    def fail_next(self, exc: Exception) -> None:
        """The next call — any method — raises `exc`, then behaviour resumes."""
        self._next_error = exc

    def _maybe_fail(self) -> None:
        if self._next_error is not None:
            error, self._next_error = self._next_error, None
            raise error

    async def get(self, key: str) -> ObjectData | None:
        self._maybe_fail()
        self.get_calls += 1
        body = self._objects.get(key)
        if body is None:
            return None
        return ObjectData(body=body, etag=_etag(body))

    async def get_if_none_match(
        self, key: str, etag: str
    ) -> ObjectData | NotModified | None:
        obj = await self.get(key)
        if obj is None:
            return None
        if obj.etag == etag:
            return NOT_MODIFIED
        return obj

    async def put(self, key: str, body: bytes, content_type: str) -> None:
        self._maybe_fail()
        self._objects[key] = body
        self.puts[key] = body

    async def head_bucket(self) -> None:
        self._maybe_fail()
