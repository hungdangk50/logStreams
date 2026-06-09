"""
LogStream — Log Normalizer Core
================================

Chuyển đổi raw log event (CloudWatch logEvents[]) thành normalized event
chuẩn hóa cho downstream (Transaction Aggregator, Detection Engine).

Input
-----
    log_event từ CloudWatch payload:
        {
            "id": "...",
            "timestamp": 1717920000000,   # epoch ms
            "message": "{...}" | "plain text"
        }

Output (normalized event)
-------------------------
    {
        "eventType": "LOG",
        "transactionId": "TXN-001",      # bắt buộc
        "traceId": "trace-bbb",          # bắt buộc
        "stepName": "PAYMENT",           # enrich + alias
        "level": "ERROR",                # DEBUG|INFO|WARN|ERROR|FATAL
        "message": "...",
        "timestamp": "2026-06-09T...",   # ISO 8601 UTC
        "serviceName": "payment-svc",    # từ payload hoặc logGroup
        "logGroup": "/aws/lambda/...",
        "logStream": "...",
        "owner": "123456789012",
        "normalizedAt": "...",
        "errorCode": "...",              # optional
        "stepStatus": "ERROR",           # optional
        "metadata": { ... }              # field còn lại sau mask PII
    }

Trả về None nếu thiếu transactionId hoặc traceId.
"""

import json
import re
from datetime import datetime, timezone
from typing import Any

from mask import mask_dict

# Map tên step không chuẩn → tên step canonical (uppercase)
STEP_ALIASES = {
    "auth": "AUTH",
    "authentication": "AUTH",
    "payment": "PAYMENT",
    "pay": "PAYMENT",
    "notify": "NOTIFY",
    "notification": "NOTIFY",
    "fraud": "FRAUD_CHECK",
    "fraud_check": "FRAUD_CHECK",
    "order": "ORDER",
    "checkout": "CHECKOUT",
}

VALID_LEVELS = frozenset({"DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL"})

# Trích service name từ log group path AWS
# Ví dụ: /aws/lambda/payment-svc → payment-svc
SERVICE_FROM_LOG_GROUP = re.compile(r"/(?:aws/)?(?:lambda|ecs|eks)/([^/]+)")

# Field đã promote lên top-level — không đưa vào metadata
_PROMOTED_FIELDS = frozenset({
    "transactionId",
    "transaction_id",
    "txnId",
    "traceId",
    "trace_id",
    "requestId",
    "step",
    "stepName",
    "stage",
    "level",
    "severity",
    "message",
    "msg",
    "timestamp",
    "serviceName",
    "service",
    "app",
})


def _first_non_empty(*values: Any) -> str | None:
    """
    Lấy giá trị string đầu tiên không rỗng từ danh sách candidates.

    Hỗ trợ nhiều tên field alias (transactionId vs transaction_id).
    """
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _normalize_level(raw_level: Any) -> str:
    """
    Chuẩn hóa log level về enum hệ thống.

    WARNING → WARN. Giá trị không hợp lệ → INFO (default an toàn).
    """
    level = str(raw_level or "INFO").strip().upper()
    if level == "WARNING":
        return "WARN"
    if level in VALID_LEVELS:
        return level
    return "INFO"


def _normalize_step(raw_step: Any) -> str | None:
    """
    Chuẩn hóa tên step: uppercase, thay -/space bằng _, áp dụng alias map.

    Ví dụ: "payment" → "PAYMENT", "fraud-check" → "FRAUD_CHECK"
    """
    step = _first_non_empty(raw_step)
    if not step:
        return None

    normalized = step.strip().upper().replace("-", "_").replace(" ", "_")
    return STEP_ALIASES.get(normalized.lower(), normalized)


def _extract_service_name(log_group: str, payload: dict[str, Any]) -> str | None:
    """
    Xác định tên microservice.

    Thứ tự ưu tiên:
        1. payload.serviceName / service / app
        2. Parse từ CloudWatch logGroup path
    """
    service = _first_non_empty(
        payload.get("serviceName"),
        payload.get("service"),
        payload.get("app"),
    )
    if service:
        return service

    match = SERVICE_FROM_LOG_GROUP.search(log_group or "")
    if match:
        return match.group(1)

    return None


def _parse_message(message: str) -> dict[str, Any]:
    """
    Parse nội dung log event message.

    - JSON object → dùng trực tiếp
    - Plain text  → wrap thành {"message": "..."}
    - Rỗng        → {}
    """
    message = message.strip()
    if not message:
        return {}

    try:
        parsed = json.loads(message)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    return {"message": message}


def _to_iso_timestamp(raw_timestamp: Any) -> str:
    """
    Chuyển timestamp về ISO 8601 UTC.

    Hỗ trợ:
        - epoch ms (> 1e12) hoặc epoch seconds
        - string ISO sẵn có
        - fallback: now() UTC
    """
    if isinstance(raw_timestamp, (int, float)):
        seconds = raw_timestamp / 1000 if raw_timestamp > 1_000_000_000_000 else raw_timestamp
        return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()

    if isinstance(raw_timestamp, str):
        return raw_timestamp

    return datetime.now(tz=timezone.utc).isoformat()


def normalize_log_event(
    log_event: dict[str, Any],
    *,
    log_group: str,
    log_stream: str,
    owner: str | None = None,
) -> dict[str, Any] | None:
    """
    Normalize một CloudWatch log event thành event chuẩn LogStream.

    Các bước:
        1. Parse message (JSON hoặc text)
        2. Mask PII (mask.py)
        3. Extract transactionId, traceId (bắt buộc)
        4. Enrich stepName, level, serviceName, timestamp
        5. Build output schema + metadata (field phụ)

    Args:
        log_event: Một phần tử từ payload["logEvents"].
        log_group: CloudWatch log group nguồn.
        log_stream: CloudWatch log stream nguồn.
        owner: AWS account ID owner log group.

    Returns:
        Normalized event dict, hoặc None nếu thiếu transactionId/traceId.
    """
    payload = _parse_message(log_event.get("message", ""))
    payload = mask_dict(payload)

    transaction_id = _first_non_empty(
        payload.get("transactionId"),
        payload.get("transaction_id"),
        payload.get("txnId"),
    )
    trace_id = _first_non_empty(
        payload.get("traceId"),
        payload.get("trace_id"),
        payload.get("requestId"),
    )

    if not transaction_id or not trace_id:
        return None

    step_name = _normalize_step(
        _first_non_empty(payload.get("step"), payload.get("stepName"), payload.get("stage"))
    )
    level = _normalize_level(payload.get("level") or payload.get("severity"))
    service_name = _extract_service_name(log_group, payload)
    timestamp = _to_iso_timestamp(
        payload.get("timestamp") or log_event.get("timestamp")
    )

    normalized = {
        "eventType": "LOG",
        "transactionId": transaction_id,
        "traceId": trace_id,
        "stepName": step_name,
        "level": level,
        "message": payload.get("message") or payload.get("msg") or "",
        "timestamp": timestamp,
        "serviceName": service_name,
        "logGroup": log_group,
        "logStream": log_stream,
        "owner": owner,
        "normalizedAt": datetime.now(tz=timezone.utc).isoformat(),
        "metadata": {
            key: value
            for key, value in payload.items()
            if key not in _PROMOTED_FIELDS
        },
    }

    if payload.get("errorCode"):
        normalized["errorCode"] = payload["errorCode"]
    if payload.get("stepStatus"):
        normalized["stepStatus"] = str(payload["stepStatus"]).upper()

    return normalized
