from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "GetObject")


def test_transport_failures_are_retryable():
    from storage.errors import is_retryable

    assert is_retryable(EndpointConnectionError(endpoint_url="http://x"))
    assert is_retryable(ConnectTimeoutError(endpoint_url="http://x"))
    assert is_retryable(ReadTimeoutError(endpoint_url="http://x"))


def test_server_side_client_errors_are_retryable():
    from storage.errors import is_retryable

    for code in ("500", "503", "SlowDown", "InternalError", "ServiceUnavailable"):
        assert is_retryable(_client_error(code)), code


def test_missing_or_forbidden_objects_are_permanent():
    from storage.errors import is_retryable

    for code in ("NoSuchKey", "NoSuchBucket", "AccessDenied", "404"):
        assert not is_retryable(_client_error(code)), code


def test_permanent_handler_error_is_not_retryable():
    from storage.errors import PermanentHandlerError, is_retryable

    assert not is_retryable(PermanentHandlerError("bad catalog"))


def test_an_unknown_exception_is_not_retryable():
    from storage.errors import is_retryable

    assert not is_retryable(ValueError("something else"))
