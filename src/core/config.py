from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: str

    RABBITMQ_URL: str

    API_KEY: str

    LOG_LEVEL: str = "INFO"

    OUTBOX_POLL_INTERVAL: float = 1.0
    OUTBOX_BATCH_SIZE: int = 100

    CONSUMER_PREFETCH: int = 10
    MAX_ATTEMPTS: int = 3
    RETRY_BASE_DELAY: float = 2.0

    PROCESSING_MIN_DELAY: float = 2.0
    PROCESSING_MAX_DELAY: float = 5.0
    PROCESSING_SUCCESS_RATE: float = 0.9

    WEBHOOK_TIMEOUT: float = 5.0

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
