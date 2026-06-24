from functools import lru_cache

from pydantic import PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    database_url: PostgresDsn
    lease_ttl_seconds: int = 30
    poll_interval_seconds: float = 1.0
    worker_concurrency: int = 4

    @field_validator("lease_ttl_seconds")
    @classmethod
    def lease_ttl_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("lease_ttl_seconds must be > 0")
        return v

    @field_validator("poll_interval_seconds")
    @classmethod
    def poll_interval_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("poll_interval_seconds must be > 0")
        return v

    @field_validator("worker_concurrency")
    @classmethod
    def concurrency_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("worker_concurrency must be > 0")
        return v


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
