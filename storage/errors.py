from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from botocore.exceptions import (
    ConnectionError as BotocoreConnectionError,
)


class PermanentHandlerError(Exception):
    """
    A failure that will fail identically on every retry.

    The consumer dead-letters this on sight instead of spending
    CONSUMER_MAX_RETRIES attempts reaching the same conclusion. Raise it for
    a missing or malformed price catalog, or an item the catalog does not
    price.
    """


# Transport-level failures: the call never reached a decision.
RETRYABLE_EXCEPTIONS = (
    EndpointConnectionError,
    ConnectTimeoutError,
    ReadTimeoutError,
    BotocoreConnectionError,
)

# ClientError covers both "the server is having a bad minute" and "this key
# does not exist", so the class alone cannot decide — only the code can.
RETRYABLE_CODES = frozenset(
    {
        "500",
        "502",
        "503",
        "504",
        "InternalError",
        "ServiceUnavailable",
        "SlowDown",
        "RequestTimeout",
        "RequestTimeTooSkewed",
    }
)


def is_retryable(exc: BaseException) -> bool:
    """
    True when the same call might succeed later.

    Mirrors messaging/outbox/backoff.py for Redis. The trap here is different:
    isinstance(exc, ClientError) is true for a 503 and for a NoSuchKey alike,
    so this branches on the response code. Anything unrecognised is treated as
    permanent — retrying a bug three times only delays the dead letter.
    """
    if isinstance(exc, RETRYABLE_EXCEPTIONS):
        return True
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        return code in RETRYABLE_CODES
    return False
