"""
LogStream — Transaction Aggregator Core (Draft)
================================================

Nhận normalized event từ Kinesis (output của Log Normalizer), buffer state
trong DynamoDB, flush lên S3 + PostgreSQL khi transaction kết thúc.

Retry support
-------------
- Mỗi attempt có traceId riêng → item ATTEMPT + LOG chunks riêng.
- STEP item = step logic (1/stepName), status aggregate (RETRYING khi có retry).
- META.lastUpdatedAt reset mỗi log → idle timeout, không fixed duration.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from constants import ATTEMPT_STATUSES, LIFECYCLE_EVENT_TYPES, TERMINAL_STEP_STATUSES
from keys import (
    attempt_sk,
    gsi1_pk_running,
    gsi2_pk,
    gsi2_sk,
    log_sk,
    meta_sk,
    step_sk,
    txn_pk,
)


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _parse_retry_context(event: dict[str, Any]) -> tuple[bool, int | None, str | None]:
    """
    Extract retry metadata from normalized event.

    Returns:
        (is_retry, attempt_number, parent_trace_id)
    """
    metadata = event.get("metadata") or {}
    attempt_raw = metadata.get("attempt")
    attempt_number = int(attempt_raw) if attempt_raw is not None else None
    is_retry = bool(metadata.get("isRetry")) or (attempt_number is not None and attempt_number > 1)
    parent_trace_id = metadata.get("parentTraceId")
    if parent_trace_id is not None:
        parent_trace_id = str(parent_trace_id).strip() or None
    return is_retry, attempt_number, parent_trace_id


def _step_status_from_event(event: dict[str, Any]) -> str | None:
    raw = event.get("stepStatus")
    if not raw:
        return None
    status = str(raw).upper()
    return status if status in TERMINAL_STEP_STATUSES | {"STARTED", "RETRYING"} else None


class TransactionAggregator:
    """
    Draft aggregator — inject DynamoDB/S3/PostgreSQL clients in production.

    Methods prefixed with `_ddb_` are placeholders for actual AWS calls.
    """

    def __init__(self, *, table_name: str, ttl_hours: int = 24) -> None:
        self.table_name = table_name
        self.ttl_hours = ttl_hours

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    def process_event(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("eventType") or "LOG").upper()
        transaction_id = event["transactionId"]

        if event_type in LIFECYCLE_EVENT_TYPES:
            final_status = "COMPLETED" if event_type == "TRANSACTION_COMPLETED" else "FAILED"
            self.flush_transaction(transaction_id, final_status=final_status)
            return

        if event_type == "LOG":
            self.handle_log_event(event)
            return

        raise ValueError(f"Unsupported eventType: {event_type}")

    def handle_log_event(self, event: dict[str, Any]) -> None:
        transaction_id = event["transactionId"]
        trace_id = event["traceId"]
        step_name = event.get("stepName") or "UNKNOWN"
        now = _now_iso()

        is_retry, attempt_number, parent_trace_id = _parse_retry_context(event)
        step_status = _step_status_from_event(event)

        self._ensure_meta(transaction_id, now=now, current_step=step_name)
        step_item, step_order = self._resolve_step(
            transaction_id,
            step_name=step_name,
            trace_id=trace_id,
            is_retry=is_retry,
            attempt_number=attempt_number,
            parent_trace_id=parent_trace_id,
            service_name=event.get("serviceName"),
            now=now,
            step_status=step_status,
        )
        self._append_log_line(
            transaction_id,
            trace_id=trace_id,
            step_name=step_name,
            event=event,
            now=now,
        )
        if step_status:
            self._update_attempt_status(
                transaction_id,
                step_order=step_order,
                step_name=step_name,
                trace_id=trace_id,
                step_status=step_status,
                error_code=event.get("errorCode"),
                error_message=event.get("message"),
                now=now,
            )
            self._reconcile_step_aggregate(
                transaction_id,
                step_name=step_name,
                step_order=step_order,
                latest_status=step_status,
                is_retry=is_retry,
                now=now,
            )

    def flush_transaction(self, transaction_id: str, *, final_status: str) -> None:
        """
        Query toàn bộ PK → ghi S3 + UPSERT PostgreSQL → xóa buffer DynamoDB.
        """
        items = self._ddb_query_txn_items(transaction_id)
        # Implementation: group ATTEMPT + LOG by traceId, write S3, upsert PG
        _ = items
        self._ddb_delete_txn_items(transaction_id)

    # ------------------------------------------------------------------
    # Step / attempt resolution
    # ------------------------------------------------------------------

    def _resolve_step(
        self,
        transaction_id: str,
        *,
        step_name: str,
        trace_id: str,
        is_retry: bool,
        attempt_number: int | None,
        parent_trace_id: str | None,
        service_name: str | None,
        now: str,
        step_status: str | None,
    ) -> tuple[dict[str, Any], int]:
        step_item = self._ddb_get_step(transaction_id, step_name)

        if step_item is None:
            step_order = self._next_step_order(transaction_id)
            self._ddb_put_step(
                transaction_id,
                step_order=step_order,
                step_name=step_name,
                latest_trace_id=trace_id,
                attempt_count=1,
                status=step_status or "STARTED",
                service_name=service_name,
                started_at=now,
            )
            self._ddb_put_attempt(
                transaction_id,
                step_order=step_order,
                step_name=step_name,
                attempt_number=1,
                trace_id=trace_id,
                parent_trace_id=None,
                is_retry=False,
                status=step_status or "STARTED",
                service_name=service_name,
                started_at=now,
            )
            return {"stepOrder": step_order}, step_order

        step_order = int(step_item["stepOrder"])
        existing_attempt = self._ddb_get_attempt_by_trace(transaction_id, trace_id)

        if existing_attempt is not None:
            return step_item, step_order

        # New attempt — retry or unexpected new traceId on same step
        next_attempt = attempt_number or (int(step_item.get("attemptCount", 1)) + 1)
        parent = parent_trace_id or step_item.get("latestTraceId")

        self._ddb_put_attempt(
            transaction_id,
            step_order=step_order,
            step_name=step_name,
            attempt_number=next_attempt,
            trace_id=trace_id,
            parent_trace_id=parent,
            is_retry=is_retry or next_attempt > 1,
            status=step_status or "STARTED",
            service_name=service_name,
            started_at=now,
        )
        self._ddb_update_step(
            transaction_id,
            step_order=step_order,
            step_name=step_name,
            updates={
                "latestTraceId": trace_id,
                "attemptCount": next_attempt,
                "status": "RETRYING" if (is_retry or next_attempt > 1) else (step_status or "STARTED"),
                "serviceName": service_name or step_item.get("serviceName"),
            },
        )
        return step_item, step_order

    def _reconcile_step_aggregate(
        self,
        transaction_id: str,
        *,
        step_name: str,
        step_order: int,
        latest_status: str,
        is_retry: bool,
        now: str,
    ) -> None:
        """
        Cập nhật STEP status từ attempt terminal.

        - SUCCESS → STEP = SUCCESS (dù trước đó ERROR ở attempt 1)
        - ERROR trên retry attempt chưa terminal → giữ RETRYING
        """
        if latest_status not in TERMINAL_STEP_STATUSES:
            return

        if latest_status == "SUCCESS":
            step_status = "SUCCESS"
        elif is_retry and latest_status == "ERROR":
            # Orchestrator có thể còn schedule attempt tiếp — giữ RETRYING
            # cho đến TRANSACTION_COMPLETED/FAILED hoặc log stepStatus SUCCESS.
            step_status = "RETRYING"
        else:
            step_status = latest_status

        updates: dict[str, Any] = {"status": step_status, "latestTraceId": None}
        if latest_status in TERMINAL_STEP_STATUSES:
            updates["endedAt"] = now
        self._ddb_update_step(
            transaction_id,
            step_order=step_order,
            step_name=step_name,
            updates={k: v for k, v in updates.items() if v is not None},
        )

    # ------------------------------------------------------------------
    # DynamoDB operations (draft stubs — replace with boto3)
    # ------------------------------------------------------------------

    def _ensure_meta(self, transaction_id: str, *, now: str, current_step: str) -> None:
        pk = txn_pk(transaction_id)
        item = self._ddb_get_item(pk, meta_sk())
        if item is None:
            ttl_epoch = int(datetime.now(tz=timezone.utc).timestamp()) + self.ttl_hours * 3600
            self._ddb_put_item({
                "pk": pk,
                "sk": meta_sk(),
                "transactionId": transaction_id,
                "status": "RUNNING",
                "currentStep": current_step,
                "stepCount": 0,
                "startedAt": now,
                "lastUpdatedAt": now,
                "completedAt": None,
                "ttl": ttl_epoch,
                "gsi1pk": gsi1_pk_running(),
                "gsi1sk": now,
            })
        else:
            self._ddb_update_item(
                pk,
                meta_sk(),
                updates={
                    "lastUpdatedAt": now,
                    "gsi1sk": now,
                    "currentStep": current_step,
                },
            )

    def _append_log_line(
        self,
        transaction_id: str,
        *,
        trace_id: str,
        step_name: str,
        event: dict[str, Any],
        now: str,
    ) -> None:
        pk = txn_pk(transaction_id)
        chunk_seq = self._next_log_chunk_seq(transaction_id, trace_id)
        sk = log_sk(trace_id, chunk_seq)
        log_line = {
            "timestamp": event.get("timestamp"),
            "level": event.get("level"),
            "message": event.get("message"),
            "transactionId": transaction_id,
            "traceId": trace_id,
            "stepName": step_name,
        }
        if event.get("errorCode"):
            log_line["errorCode"] = event["errorCode"]
        if event.get("stepStatus"):
            log_line["stepStatus"] = event["stepStatus"]

        payload = json.dumps(log_line, separators=(",", ":"), ensure_ascii=False)
        self._ddb_put_item({
            "pk": pk,
            "sk": sk,
            "traceId": trace_id,
            "stepName": step_name,
            "chunkSeq": chunk_seq,
            "logLines": [log_line],
            "lineCount": 1,
            "byteSize": len(payload.encode("utf-8")),
            "createdAt": now,
        })

    def _update_attempt_status(
        self,
        transaction_id: str,
        *,
        step_order: int,
        step_name: str,
        trace_id: str,
        step_status: str,
        error_code: str | None,
        error_message: str | None,
        now: str,
    ) -> None:
        if step_status not in ATTEMPT_STATUSES:
            return
        attempt = self._ddb_get_attempt_by_trace(transaction_id, trace_id)
        if attempt is None:
            return
        attempt_number = int(attempt["attemptNumber"])
        updates: dict[str, Any] = {"status": step_status}
        if step_status in TERMINAL_STEP_STATUSES:
            updates["endedAt"] = now
        if error_code:
            updates["errorCode"] = error_code
        if error_message and step_status == "ERROR":
            updates["errorMessage"] = error_message
        self._ddb_update_item(
            txn_pk(transaction_id),
            attempt_sk(step_order, step_name, attempt_number),
            updates=updates,
        )

    def _next_step_order(self, transaction_id: str) -> int:
        steps = self._ddb_list_steps(transaction_id)
        if not steps:
            return 1
        return max(int(s["stepOrder"]) for s in steps) + 1

    def _next_log_chunk_seq(self, transaction_id: str, trace_id: str) -> int:
        chunks = self._ddb_list_log_chunks(transaction_id, trace_id)
        if not chunks:
            return 1
        return max(int(c["chunkSeq"]) for c in chunks) + 1

    # --- boto3 placeholders ---

    def _ddb_get_item(self, pk: str, sk: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def _ddb_put_item(self, item: dict[str, Any]) -> None:
        raise NotImplementedError

    def _ddb_update_item(self, pk: str, sk: str, *, updates: dict[str, Any]) -> None:
        raise NotImplementedError

    def _ddb_get_step(self, transaction_id: str, step_name: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def _ddb_put_step(self, transaction_id: str, **fields: Any) -> None:
        pk = txn_pk(transaction_id)
        step_order = fields["step_order"]
        step_name = fields["step_name"]
        self._ddb_put_item({
            "pk": pk,
            "sk": step_sk(step_order, step_name),
            "stepName": step_name,
            "stepOrder": step_order,
            "latestTraceId": fields["latest_trace_id"],
            "attemptCount": fields["attempt_count"],
            "status": fields["status"],
            "serviceName": fields.get("service_name"),
            "startedAt": fields.get("started_at"),
            "endedAt": None,
            "logChunkCount": 0,
        })

    def _ddb_update_step(
        self,
        transaction_id: str,
        *,
        step_order: int,
        step_name: str,
        updates: dict[str, Any],
    ) -> None:
        self._ddb_update_item(txn_pk(transaction_id), step_sk(step_order, step_name), updates=updates)

    def _ddb_put_attempt(self, transaction_id: str, **fields: Any) -> None:
        pk = txn_pk(transaction_id)
        step_order = fields["step_order"]
        step_name = fields["step_name"]
        attempt_number = fields["attempt_number"]
        trace_id = fields["trace_id"]
        self._ddb_put_item({
            "pk": pk,
            "sk": attempt_sk(step_order, step_name, attempt_number),
            "stepName": step_name,
            "stepOrder": step_order,
            "attemptNumber": attempt_number,
            "traceId": trace_id,
            "parentTraceId": fields.get("parent_trace_id"),
            "isRetry": fields.get("is_retry", False),
            "status": fields["status"],
            "serviceName": fields.get("service_name"),
            "startedAt": fields.get("started_at"),
            "endedAt": None,
            "logChunkCount": 0,
            "gsi2pk": gsi2_pk(trace_id),
            "gsi2sk": gsi2_sk(transaction_id),
        })

    def _ddb_get_attempt_by_trace(self, transaction_id: str, trace_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    def _ddb_list_steps(self, transaction_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _ddb_list_log_chunks(self, transaction_id: str, trace_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _ddb_query_txn_items(self, transaction_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _ddb_delete_txn_items(self, transaction_id: str) -> None:
        raise NotImplementedError
