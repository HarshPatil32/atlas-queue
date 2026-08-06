import random

from core.config import get_settings


def compute_backoff_seconds(
    attempt: int,
    *,
    base_delay_seconds: float,
    multiplier: float,
    max_delay_seconds: float,
    jitter_ratio: float = 0.0,
) -> float:
    """Delay before retrying the given attempt number (1-indexed)."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")

    try:
        delay = base_delay_seconds * (multiplier ** (attempt - 1))
    except OverflowError:
        delay = max_delay_seconds
    else:
        delay = min(delay, max_delay_seconds)

    if jitter_ratio:
        delay *= 1.0 + random.uniform(-jitter_ratio, jitter_ratio)
        delay = max(0.0, min(delay, max_delay_seconds))

    return delay


def retry_delay_seconds(attempts: int) -> float:
    """Convenience wrapper reading backoff config from settings."""
    settings = get_settings()
    return compute_backoff_seconds(
        attempts,
        base_delay_seconds=settings.retry_backoff_base_seconds,
        multiplier=settings.retry_backoff_multiplier,
        max_delay_seconds=settings.retry_backoff_max_seconds,
        jitter_ratio=settings.retry_backoff_jitter_ratio,
    )
