from core.models import JobStatus


def zero_status_counts() -> dict[str, int]:
    return {status.value: 0 for status in JobStatus}
