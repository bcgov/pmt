# config/settings.py
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # -------------------------
    # Environment Metadata
    # -------------------------
    ENVIRONMENT: str = "development"  # development | staging | production
    SERVICE_NAME: str = "python-microservice-template"
    SERVICE_VERSION: str = "0.1.0"

    # SERVICE_NAME / SERVICE_VERSION / ENVIRONMENT have exactly one consumer
    # now: the OpenTelemetry Resource in config/tracing.py, which stamps them
    # on every exported span. The /info endpoint that used to serve them is
    # gone — the collector has the same three values.

    # -------------------------
    # Health probe server
    # -------------------------
    HEALTH_PORT: int = 8000

    # -------------------------
    # Handler state store
    # -------------------------
    STATE_TTL_SECONDS: int = 3600

    # -------------------------
    # Redis Streams Messaging
    # -------------------------
    REDIS_STREAM_URL: str = "redis://localhost:6379/1"
    STREAM_NAME: str = "order.events"
    CONSUMER_GROUP: str = "order_service_v1"
    STREAM_POLL_INTERVAL_MS: int = 1000  # XREADGROUP BLOCK duration

    # -------------------------
    # Consumer reliability
    # -------------------------
    CONSUMER_BATCH_SIZE: int = 10
    CONSUMER_MAX_RETRIES: int = 3
    CONSUMER_RETRY_BACKOFF_MS: int = 500
    CONSUMER_CLAIM_MIN_IDLE_MS: int = 60_000
    DLQ_STREAM_NAME: str = ""  # empty -> derived from STREAM_NAME

    # -------------------------
    # Logging
    # -------------------------
    LOG_LEVEL: str = "INFO"
    JSON_LOGS: bool = False

    # -------------------------
    # OpenTelemetry / Tracing
    # -------------------------
    OTEL_EXPORTER_OTLP_ENDPOINT: Optional[str] = None
    OTEL_EXPORTER_OTLP_ENDPOINT_ENABLE_FALLBACK: bool = False

    # -------------------------
    # Pydantic Config
    # -------------------------
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def dlq_stream(self) -> str:
        """Dead-letter stream; defaults to '<STREAM_NAME>:dlq'."""
        return self.DLQ_STREAM_NAME or f"{self.STREAM_NAME}:dlq"


@lru_cache
def get_settings() -> Settings:
    return Settings()
