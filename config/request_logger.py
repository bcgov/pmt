# config/request_logger.py

import time

import structlog
from opentelemetry import trace
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

logger = structlog.get_logger("request_logger")
tracer = trace.get_tracer("pmt.request")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware that wraps each request in an OpenTelemetry span
    and logs the request in JSON format.
    """

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()

        # Span name like "GET /info"
        span_name = f"{request.method} {request.url.path}"

        with tracer.start_as_current_span(span_name) as span:
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.target", str(request.url.path))
            span.set_attribute("http.query", str(request.url.query or ""))
            span.set_attribute("http.scheme", request.url.scheme)

            response = await call_next(request)

            duration = round((time.time() - start_time) * 1000, 2)

            span.set_attribute("http.status_code", response.status_code)
            span.set_attribute("http.server_duration_ms", duration)

            logger.info(
                "request_completed",
                method=request.method,
                path=request.url.path,
                query=str(request.url.query or ""),
                status=response.status_code,
                duration_ms=duration,
            )

            return response
