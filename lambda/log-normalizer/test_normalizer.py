"""
LogStream — Unit tests cho Log Normalizer
==========================================

Chạy:
    cd lambda/log-normalizer
    python -m unittest test_normalizer.py -v
"""

import json
import unittest

from normalizer import normalize_log_event


class NormalizeLogEventTests(unittest.TestCase):
    """Test cases cho normalize_log_event()."""

    def test_normalize_json_log(self):
        """Log JSON hợp lệ → normalized event đủ field + PII masked."""
        log_event = {
            "timestamp": 1_717_920_000_000,
            "message": json.dumps(
                {
                    "transactionId": "TXN-001",
                    "traceId": "trace-aaa",
                    "step": "payment",
                    "level": "error",
                    "message": "Payment declined",
                    "errorCode": "PAYMENT_DECLINED",
                    "email": "user@example.com",
                }
            ),
        }

        result = normalize_log_event(
            log_event,
            log_group="/aws/lambda/payment-svc",
            log_stream="2026/06/09/[$LATEST]abc",
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["transactionId"], "TXN-001")
        self.assertEqual(result["traceId"], "trace-aaa")
        self.assertEqual(result["stepName"], "PAYMENT")
        self.assertEqual(result["level"], "ERROR")
        self.assertEqual(result["serviceName"], "payment-svc")
        self.assertEqual(result["metadata"]["email"], "***MASKED***")

    def test_missing_ids_returns_none(self):
        """Log thiếu transactionId/traceId → trả về None."""
        log_event = {
            "timestamp": 1_717_920_000_000,
            "message": json.dumps({"message": "hello"}),
        }

        result = normalize_log_event(
            log_event,
            log_group="/aws/lambda/order-svc",
            log_stream="stream-1",
        )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
