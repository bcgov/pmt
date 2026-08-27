from typing import Optional

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)

from config.settings import get_settings

_tracer_initialized = False


def init_tracing(app: Optional[FastAPI] = None) -> None:
    """
    Initialize OpenTelemetry tracing and instrument FastAPI.
    Safe to call multiple times (idempotent).
    """
    global _tracer_initialized
    if _tracer_initialized:
        return

    settings = get_settings()

    resource = Resource.create(
        {
            "service.name": settings.SERVICE_NAME,
            "service.version": settings.SERVICE_VERSION,
            "deployment.environment": settings.ENVIRONMENT,
        }
    )

    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    # OTLP HTTP exporter (to OTEL collector)
    raw_endpoint = settings.OTEL_EXPORTER_OTLP_ENDPOINT
    endpoint = (raw_endpoint or "").strip() if raw_endpoint is not None else ""

    if endpoint and endpoint.lower() != "disabled":
        # Send spans to OTEL collector
        span_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
        provider.add_span_processor(span_processor)
    elif settings.OTEL_EXPORTER_OTLP_ENDPOINT_ENABLE_FALLBACK:
        # Fallback: log spans to console (useful in dev)
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))

    # Instrument FastAPI app if provided
    if app is not None:
        FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)

    _tracer_initialized = True
