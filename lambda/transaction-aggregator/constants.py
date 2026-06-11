"""Shared constants for Transaction Aggregator."""

from typing import Final

TABLE_NAME_ENV = "DYNAMODB_TABLE_NAME"
S3_BUCKET_ENV = "S3_ARCHIVE_BUCKET"
IDLE_TIMEOUT_SECONDS_ENV = "IDLE_TIMEOUT_SECONDS"

DEFAULT_IDLE_TIMEOUT_SECONDS: Final[int] = 30 * 60
LOG_CHUNK_MAX_BYTES: Final[int] = 300 * 1024

TERMINAL_STEP_STATUSES: Final[frozenset[str]] = frozenset({
    "SUCCESS", "ERROR", "TIMEOUT", "SKIPPED",
})

LIFECYCLE_EVENT_TYPES: Final[frozenset[str]] = frozenset({
    "TRANSACTION_COMPLETED",
    "TRANSACTION_FAILED",
})

STEP_STATUSES: Final[frozenset[str]] = frozenset({
    "STARTED", "RETRYING", "SUCCESS", "ERROR", "TIMEOUT", "SKIPPED",
})

ATTEMPT_STATUSES: Final[frozenset[str]] = frozenset({
    "STARTED", "SUCCESS", "ERROR", "TIMEOUT", "SKIPPED",
})
