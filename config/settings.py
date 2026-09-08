# config/settings.py
from datetime import date
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
    SERVICE_PORT: int = 8000

    # -------------------------
    # Database Configuration
    # -------------------------
    DATABASE_URL: str

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
    # Outbox relay
    # -------------------------
    RELAY_ENABLED: bool = True
    OUTBOX_POLL_INTERVAL_MS: int = 200
    # Also caps duplicate amplification: a crash mid-batch republishes at most
    # this many rows.
    OUTBOX_BATCH_SIZE: int = 20
    OUTBOX_RETRY_BACKOFF_MS: int = 500
    OUTBOX_MAX_BACKOFF_MS: int = 30_000
    OUTBOX_RETENTION_HOURS: int = 24
    OUTBOX_SWEEP_INTERVAL_S: int = 300

    # -------------------------
    # Object storage (S3 API)
    # -------------------------
    S3_ENDPOINT_URL: str = "http://localhost:8333"
    S3_REGION: str = "us-east-1"
    S3_BUCKET: str = "pmt-bucket"
    S3_ACCESS_KEY_ID: str = "dev"
    S3_SECRET_ACCESS_KEY: str = "dev"
    S3_PRICES_KEY: str = "config/prices.json"
    S3_ROLLUP_PREFIX: str = "rollups/"
    S3_CONNECT_TIMEOUT_S: int = 2
    S3_READ_TIMEOUT_S: int = 5
    PRICES_CACHE_TTL_S: int = 60

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

    def rollup_key(self, day: date) -> str:
        """Object key for one day's rollup: '<prefix><YYYY-MM-DD>.json'."""
        return f"{self.S3_ROLLUP_PREFIX}{day.isoformat()}.json"


@lru_cache
def get_settings() -> Settings:
    return Settings()
