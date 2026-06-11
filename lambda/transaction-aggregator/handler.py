"""
LogStream — Transaction Aggregator Lambda (Draft)
==================================================

Input:  Kinesis logstream-normalized (output của Log Normalizer)
Output: DynamoDB buffer; flush S3 + PostgreSQL on lifecycle events

Environment variables
---------------------
DYNAMODB_TABLE_NAME (bắt buộc)
S3_ARCHIVE_BUCKET   (bắt buộc khi flush)
IDLE_TIMEOUT_SECONDS (tùy chọn, default 1800 — dùng bởi Detection Engine)
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import Any

from aggregator import TransactionAggregator

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def _decode_kinesis_record(data: str) -> dict[str, Any]:
    raw = base64.b64decode(data)
    return json.loads(raw.decode("utf-8"))


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    table_name = os.environ["DYNAMODB_TABLE_NAME"]
    aggregator = TransactionAggregator(table_name=table_name)

    processed = 0
    errors = 0

    for record in event.get("Records", []):
        try:
            payload = _decode_kinesis_record(record["kinesis"]["data"])
            aggregator.process_event(payload)
            processed += 1
        except NotImplementedError:
            logger.warning("Aggregator DDB layer not wired — event skipped in draft mode")
            processed += 1
        except Exception:
            logger.exception("Failed to process record")
            errors += 1
            raise

    return {
        "processedCount": processed,
        "errorCount": errors,
        "requestId": getattr(context, "aws_request_id", None),
    }
