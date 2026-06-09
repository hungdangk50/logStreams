"""
LogStream — Lambda Log Normalizer (Entry Point)
=================================================

Vị trí trong pipeline
-----------------------
    CloudWatch Logs
        → Subscription Filter
        → Kinesis (input: logstream-raw)
        → [Lambda này] Log Normalizer
        → Kinesis (output: logstream-normalized)
        → Transaction Aggregator / Detection Engine

Chức năng
---------
1. Nhận batch record từ Kinesis event source mapping.
2. Decode payload CloudWatch Logs subscription (base64 → gzip → JSON).
3. Gọi normalizer.py để parse, mask PII, enrich metadata từng log event.
4. Ghi event đã chuẩn hóa lên Kinesis output stream.
5. Ghi event invalid (thiếu transactionId/traceId) lên DLQ stream (nếu cấu hình).

Environment variables
---------------------
OUTPUT_STREAM_NAME (bắt buộc)
    Tên Kinesis stream nhận log đã normalize.
    Partition key = transactionId (đảm bảo cùng transaction vào cùng shard).

DLQ_STREAM_NAME (tùy chọn)
    Stream dead-letter cho log không parse được hoặc thiếu ID bắt buộc.
    Nếu không set, invalid records chỉ bị bỏ qua (không ghi DLQ).

LOG_LEVEL (tùy chọn, default: INFO)
    Mức log CloudWatch của chính Lambda này.

AWS_REGION (tự inject bởi Lambda runtime)
    Region dùng cho boto3 Kinesis client.

Handler
-------
    handler.handler  ← cấu hình trên AWS Lambda

Input event
-----------
    AWS Kinesis event chuẩn, mỗi record["kinesis"]["data"] là base64 của
    CloudWatch Logs subscription payload (thường nén gzip).

Output
------
    dict với normalizedCount, invalidCount, requestId.

IAM permissions cần thiết
-------------------------
    kinesis:GetRecords, kinesis:GetShardIterator, kinesis:DescribeStream
    kinesis:PutRecords, kinesis:PutRecord
    logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents
"""

import base64
import gzip
import json
import logging
import os
from typing import Any

import boto3

from normalizer import normalize_log_event

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Kinesis stream output — bắt buộc, Lambda fail fast nếu thiếu
OUTPUT_STREAM_NAME = os.environ["OUTPUT_STREAM_NAME"]
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
# DLQ optional — chỉ ghi khi có giá trị
DLQ_STREAM_NAME = os.getenv("DLQ_STREAM_NAME")

_kinesis = boto3.client("kinesis", region_name=AWS_REGION)


def _decode_kinesis_data(data: str) -> dict[str, Any]:
    """
    Giải mã dữ liệu từ Kinesis record.

    CloudWatch Logs subscription filter gửi payload dạng:
        base64( gzip( JSON CloudWatch payload ) )

    Args:
        data: Chuỗi base64 từ event["Records"][i]["kinesis"]["data"].

    Returns:
        Dict CloudWatch payload gồm messageType, logGroup, logStream, logEvents, ...

    Raises:
        json.JSONDecodeError: Payload không phải JSON hợp lệ.
        OSError: Lỗi gzip decompress.
    """
    raw = base64.b64decode(data)

    # Magic bytes gzip: 0x1f 0x8b — CloudWatch subscription luôn nén gzip
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)

    return json.loads(raw.decode("utf-8"))


def _put_records(stream_name: str, records: list[dict[str, Any]]) -> None:
    """
    Ghi danh sách record lên Kinesis stream.

    - PartitionKey = transactionId → cùng transaction luôn vào cùng shard,
      giúp Transaction Aggregator xử lý tuần tự theo transaction.
    - Batch tối đa 500 records/lần (giới hạn PutRecords API).

    Args:
        stream_name: Tên Kinesis stream đích.
        records: List dict event (normalized hoặc invalid/DLQ).

    Raises:
        RuntimeError: Có record failed trong response PutRecords.
    """
    if not records:
        return

    entries = []
    for record in records:
        partition_key = record["transactionId"]
        entries.append(
            {
                "Data": json.dumps(record, separators=(",", ":"), ensure_ascii=False),
                "PartitionKey": partition_key,
            }
        )

    for offset in range(0, len(entries), 500):
        batch = entries[offset : offset + 500]
        response = _kinesis.put_records(StreamName=stream_name, Records=batch)
        failed = response.get("FailedRecordCount", 0)
        if failed:
            logger.error(
                "Failed to publish %s records to %s",
                failed,
                stream_name,
                extra={"response": response},
            )
            raise RuntimeError(f"Kinesis PutRecords failed: {failed} record(s)")


def _process_cloudwatch_payload(payload: dict[str, Any]) -> tuple[list[dict], list[dict]]:
    """
    Xử lý một CloudWatch Logs subscription payload.

    messageType:
        DATA_MESSAGE    → xử lý từng logEvents
        CONTROL_MESSAGE → bỏ qua (health check subscription)
        khác            → log warning, bỏ qua

    Args:
        payload: CloudWatch payload sau decode.

    Returns:
        Tuple (normalized_records, invalid_records):
            - normalized_records: event đủ transactionId + traceId
            - invalid_records: event thiếu ID, kèm reason + rawMessage
    """
    message_type = payload.get("messageType")
    if message_type == "CONTROL_MESSAGE":
        logger.info("Received CloudWatch control message")
        return [], []

    if message_type != "DATA_MESSAGE":
        logger.warning("Unsupported messageType: %s", message_type)
        return [], []

    log_group = payload.get("logGroup", "")
    log_stream = payload.get("logStream", "")
    owner = payload.get("owner")
    normalized_records: list[dict[str, Any]] = []
    invalid_records: list[dict[str, Any]] = []

    for log_event in payload.get("logEvents", []):
        normalized = normalize_log_event(
            log_event,
            log_group=log_group,
            log_stream=log_stream,
            owner=owner,
        )
        if normalized:
            normalized_records.append(normalized)
            continue

        # Thiếu transactionId hoặc traceId — không thể route downstream
        invalid_records.append(
            {
                "reason": "MISSING_TRANSACTION_OR_TRACE",
                "logGroup": log_group,
                "logStream": log_stream,
                "rawMessage": log_event.get("message"),
                "timestamp": log_event.get("timestamp"),
            }
        )

    return normalized_records, invalid_records


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    AWS Lambda entry point — Log Normalizer.

    Luồng xử lý:
        1. Duyệt event["Records"] từ Kinesis trigger.
        2. Decode từng record → CloudWatch payload.
        3. Normalize tất cả logEvents trong payload.
        4. PutRecords lên OUTPUT_STREAM_NAME.
        5. PutRecords invalid lên DLQ_STREAM_NAME (nếu có).

    Args:
        event: Kinesis event batch từ AWS Lambda trigger.
        context: Lambda context (aws_request_id, ...).

    Returns:
        {
            "normalizedCount": int,  # số event ghi output stream
            "invalidCount": int,     # số event invalid / DLQ
            "requestId": str | None  # Lambda request ID
        }

    Raises:
        RuntimeError: PutRecords thất bại (Lambda retry theo cấu hình).
        KeyError: Thiếu OUTPUT_STREAM_NAME env var (fail tại cold start).
    """
    normalized_batch: list[dict[str, Any]] = []
    invalid_batch: list[dict[str, Any]] = []

    for record in event.get("Records", []):
        payload = _decode_kinesis_data(record["kinesis"]["data"])
        normalized, invalid = _process_cloudwatch_payload(payload)
        normalized_batch.extend(normalized)
        invalid_batch.extend(invalid)

    _put_records(OUTPUT_STREAM_NAME, normalized_batch)

    if invalid_batch and DLQ_STREAM_NAME:
        _put_records(DLQ_STREAM_NAME, invalid_batch)

    result = {
        "normalizedCount": len(normalized_batch),
        "invalidCount": len(invalid_batch),
        "requestId": getattr(context, "aws_request_id", None),
    }
    logger.info("Normalizer batch complete", extra=result)
    return result
