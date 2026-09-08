from redis.exceptions import (
    BusyLoadingError,
    ReadOnlyError,
    ResponseError,
)
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
)
from redis.exceptions import (
    TimeoutError as RedisTimeoutError,
)

from messaging.outbox.backoff import backoff_ms, is_retryable


def test_backoff_doubles_with_each_attempt():
    assert backoff_ms(0, base_ms=500, cap_ms=30_000) == 500
    assert backoff_ms(1, base_ms=500, cap_ms=30_000) == 1000
    assert backoff_ms(2, base_ms=500, cap_ms=30_000) == 2000
    assert backoff_ms(3, base_ms=500, cap_ms=30_000) == 4000


def test_backoff_is_capped():
    assert backoff_ms(20, base_ms=500, cap_ms=30_000) == 30_000


def test_transport_failures_are_retryable():
    for exc in (
        RedisConnectionError("down"),
        RedisTimeoutError("slow"),
        BusyLoadingError("loading"),
        ReadOnlyError("replica"),
    ):
        assert is_retryable(exc) is True


def test_response_errors_are_permanent():
    """
    WRONGTYPE or an oversized payload will never succeed on retry. Treating
    them as retryable would head-of-line block every row behind them.
    """
    assert is_retryable(ResponseError("WRONGTYPE")) is False


def test_unexpected_exceptions_are_permanent():
    assert is_retryable(ValueError("nonsense")) is False


def test_readonly_error_is_retryable_despite_subclassing_response_error():
    """
    redis-py's ReadOnlyError and BusyLoadingError both inherit ResponseError,
    so the retryable check must come first or a failover would dead-letter
    every row in flight.
    """
    assert issubclass(ReadOnlyError, ResponseError)
    assert is_retryable(ReadOnlyError("replica")) is True
