"""Unit tests for retry/attempt logic in Transaction Aggregator (in-memory stub)."""

import unittest
from typing import Any

from aggregator import TransactionAggregator
from keys import attempt_sk, log_sk, meta_sk, step_sk, txn_pk


class InMemoryAggregator(TransactionAggregator):
    """In-memory DynamoDB stub for draft tests."""

    def __init__(self) -> None:
        super().__init__(table_name="test")
        self.items: dict[tuple[str, str], dict[str, Any]] = {}

    def _ddb_get_item(self, pk: str, sk: str) -> dict[str, Any] | None:
        return self.items.get((pk, sk))

    def _ddb_put_item(self, item: dict[str, Any]) -> None:
        self.items[(item["pk"], item["sk"])] = dict(item)

    def _ddb_update_item(self, pk: str, sk: str, *, updates: dict[str, Any]) -> None:
        key = (pk, sk)
        if key not in self.items:
            raise KeyError(key)
        self.items[key].update(updates)

    def _ddb_get_step(self, transaction_id: str, step_name: str) -> dict[str, Any] | None:
        pk = txn_pk(transaction_id)
        for (item_pk, sk), item in self.items.items():
            if item_pk == pk and sk.startswith("STEP#") and item.get("stepName") == step_name:
                return item
        return None

    def _ddb_get_attempt_by_trace(self, transaction_id: str, trace_id: str) -> dict[str, Any] | None:
        pk = txn_pk(transaction_id)
        for (item_pk, sk), item in self.items.items():
            if item_pk == pk and sk.startswith("ATTEMPT#") and item.get("traceId") == trace_id:
                return item
        return None

    def _ddb_list_steps(self, transaction_id: str) -> list[dict[str, Any]]:
        pk = txn_pk(transaction_id)
        return [item for (item_pk, sk), item in self.items.items() if item_pk == pk and sk.startswith("STEP#")]

    def _ddb_list_log_chunks(self, transaction_id: str, trace_id: str) -> list[dict[str, Any]]:
        pk = txn_pk(transaction_id)
        prefix = f"LOG#{trace_id}#"
        return [
            item
            for (item_pk, sk), item in self.items.items()
            if item_pk == pk and sk.startswith(prefix)
        ]

    def _ddb_query_txn_items(self, transaction_id: str) -> list[dict[str, Any]]:
        pk = txn_pk(transaction_id)
        return [item for (item_pk, _), item in self.items.items() if item_pk == pk]

    def _ddb_delete_txn_items(self, transaction_id: str) -> None:
        pk = txn_pk(transaction_id)
        for key in [k for k in self.items if k[0] == pk]:
            del self.items[key]


class RetryAttemptTests(unittest.TestCase):
    def test_first_attempt_creates_step_and_attempt(self) -> None:
        agg = InMemoryAggregator()
        agg.handle_log_event({
            "eventType": "LOG",
            "transactionId": "TXN-001",
            "traceId": "trace-bbb",
            "stepName": "PAYMENT",
            "level": "INFO",
            "message": "Payment started",
            "timestamp": "2026-06-09T10:00:03.000Z",
            "stepStatus": "STARTED",
        })

        pk = txn_pk("TXN-001")
        meta = agg.items[(pk, meta_sk())]
        step = agg.items[(pk, step_sk(1, "PAYMENT"))]
        attempt = agg.items[(pk, attempt_sk(1, "PAYMENT", 1))]
        log_item = agg.items[(pk, log_sk("trace-bbb", 1))]

        self.assertEqual(meta["status"], "RUNNING")
        self.assertEqual(step["attemptCount"], 1)
        self.assertEqual(step["latestTraceId"], "trace-bbb")
        self.assertEqual(attempt["isRetry"], False)
        self.assertEqual(log_item["traceId"], "trace-bbb")

    def test_retry_creates_second_attempt_and_step_retrying(self) -> None:
        agg = InMemoryAggregator()
        base = {
            "eventType": "LOG",
            "transactionId": "TXN-001",
            "stepName": "PAYMENT",
            "level": "INFO",
            "timestamp": "2026-06-09T10:00:03.000Z",
        }
        agg.handle_log_event({**base, "traceId": "trace-bbb", "message": "fail", "stepStatus": "ERROR"})
        agg.handle_log_event({
            **base,
            "traceId": "trace-retry-002",
            "message": "retry 2",
            "stepStatus": "STARTED",
            "metadata": {"isRetry": True, "attempt": 2, "parentTraceId": "trace-bbb"},
        })

        pk = txn_pk("TXN-001")
        step = agg.items[(pk, step_sk(1, "PAYMENT"))]
        attempt2 = agg.items[(pk, attempt_sk(1, "PAYMENT", 2))]

        self.assertEqual(step["status"], "RETRYING")
        self.assertEqual(step["attemptCount"], 2)
        self.assertEqual(step["latestTraceId"], "trace-retry-002")
        self.assertTrue(attempt2["isRetry"])
        self.assertEqual(attempt2["parentTraceId"], "trace-bbb")
        self.assertIn((pk, log_sk("trace-retry-002", 1)), agg.items)

    def test_log_resets_meta_last_updated(self) -> None:
        agg = InMemoryAggregator()
        agg.handle_log_event({
            "eventType": "LOG",
            "transactionId": "TXN-001",
            "traceId": "trace-1",
            "stepName": "AUTH",
            "level": "INFO",
            "message": "ok",
            "timestamp": "2026-06-09T10:00:00.000Z",
        })
        first_updated = agg.items[(txn_pk("TXN-001"), meta_sk())]["lastUpdatedAt"]

        agg.handle_log_event({
            "eventType": "LOG",
            "transactionId": "TXN-001",
            "traceId": "trace-1",
            "stepName": "AUTH",
            "level": "DEBUG",
            "message": "heartbeat",
            "timestamp": "2026-06-09T10:30:00.000Z",
            "metadata": {"heartbeat": True},
        })
        second_updated = agg.items[(txn_pk("TXN-001"), meta_sk())]["lastUpdatedAt"]
        self.assertNotEqual(first_updated, second_updated)
        self.assertEqual(agg.items[(txn_pk("TXN-001"), meta_sk())]["gsi1sk"], second_updated)


if __name__ == "__main__":
    unittest.main()
