from redis.exceptions import (
    BusyLoadingError,
    ReadOnlyError,
)
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
)
from redis.exceptions import (
    TimeoutError as RedisTimeoutError,
)

# Redis is unreachable or busy: the event is fine and must eventually go out.
# ReadOnlyError and BusyLoadingError both subclass ResponseError, so this
# tuple has to be checked before any ResponseError handling.
RETRYABLE = (
    RedisConnectionError,
    RedisTimeoutError,
    BusyLoadingError,
    ReadOnlyError,
)


def is_retryable(exc: BaseException) -> bool:
    """
    True when publishing might succeed later.

    Everything else — a ResponseError like WRONGTYPE, a payload over
    proto-max-bulk-len, an unexpected bug — will fail identically forever, so
    the relay marks it failed and moves on rather than blocking the rows
    behind it.
    """
    return isinstance(exc, RETRYABLE)


def backoff_ms(attempts: int, base_ms: int, cap_ms: int) -> int:
    """
    Exponential backoff for transport failures. `attempts` is the count before
    this failure is recorded, so the first retry waits `base_ms`.
    """
    return min(base_ms * (2**attempts), cap_ms)
