import logging
from functools import lru_cache

from pydantic import PostgresDsn, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from core.logging import LogFormat


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    database_url: PostgresDsn
    lease_ttl_seconds: int = 30
    lease_heartbeat_interval_seconds: float = 10.0
    poll_interval_seconds: float = 1.0
    poll_backoff_multiplier: float = 2.0
    poll_backoff_max_seconds: float = 30.0
    reaper_interval_seconds: float = 30.0
    worker_concurrency: int = 4
    log_level: str = "INFO"
    log_format: LogFormat = "json"

    @field_validator("log_level")
    @classmethod
    def log_level_valid(cls, v: str) -> str:
        normalized = v.upper()
        if normalized not in logging.getLevelNamesMapping():
            valid = ", ".join(sorted(logging.getLevelNamesMapping()))
            raise ValueError(f"log_level must be one of: {valid}")
        return normalized

    @field_validator("log_format")
    @classmethod
    def log_format_valid(cls, v: str) -> str:
        if v not in {"json", "console"}:
            raise ValueError('log_format must be "json" or "console"')
        return v

    @field_validator("lease_ttl_seconds")
    @classmethod
    def lease_ttl_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("lease_ttl_seconds must be > 0")
        return v

    @field_validator("lease_heartbeat_interval_seconds")
    @classmethod
    def lease_heartbeat_interval_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("lease_heartbeat_interval_seconds must be > 0")
        return v

    @field_validator("poll_interval_seconds")
    @classmethod
    def poll_interval_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("poll_interval_seconds must be > 0")
        return v

    @field_validator("poll_backoff_multiplier")
    @classmethod
    def poll_backoff_multiplier_valid(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("poll_backoff_multiplier must be >= 1.0")
        return v

    @field_validator("poll_backoff_max_seconds")
    @classmethod
    def poll_backoff_max_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("poll_backoff_max_seconds must be > 0")
        return v

    @field_validator("reaper_interval_seconds")
    @classmethod
    def reaper_interval_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("reaper_interval_seconds must be > 0")
        return v

    @field_validator("worker_concurrency")
    @classmethod
    def concurrency_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("worker_concurrency must be > 0")
        return v

    @model_validator(mode="after")
    def lease_heartbeat_interval_less_than_ttl(self) -> "Settings":
        if self.lease_heartbeat_interval_seconds >= self.lease_ttl_seconds:
            raise ValueError(
                "lease_heartbeat_interval_seconds must be < lease_ttl_seconds"
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
