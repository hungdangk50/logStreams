"""
LogStream — Generate Sample Kinesis Event
==========================================

Tạo file events/sample-kinesis-event.json để test Lambda Log Normalizer
qua AWS CLI invoke hoặc SAM local.

Cách chạy:
    cd lambda/log-normalizer
    python generate_sample_event.py

Output:
    events/sample-kinesis-event.json

Format mô phỏng:
    Kinesis record.data = base64( gzip( CloudWatch subscription payload ) )
    Giống dữ liệu thật từ CloudWatch Logs → Kinesis subscription filter.
"""

import base64
import gzip
import json
from pathlib import Path

# Payload CloudWatch Logs subscription (DATA_MESSAGE)
CW_PAYLOAD = {
    "messageType": "DATA_MESSAGE",
    "owner": "123456789012",
    "logGroup": "/aws/lambda/payment-svc",
    "logStream": "2026/06/09/[$LATEST]abc",
    "subscriptionFilters": ["logstream-to-kinesis"],
    "logEvents": [
        {
            "id": "1",
            "timestamp": 1717920000000,
            "message": json.dumps(
                {
                    "transactionId": "TXN-001",
                    "traceId": "trace-bbb",
                    "step": "PAYMENT",
                    "level": "ERROR",
                    "message": "Payment declined",
                    "errorCode": "PAYMENT_DECLINED",
                }
            ),
        }
    ],
}

compressed = gzip.compress(json.dumps(CW_PAYLOAD).encode("utf-8"))
kinesis_event = {
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "TXN-001",
                "sequenceNumber": "1",
                "data": base64.b64encode(compressed).decode("utf-8"),
                "approximateArrivalTimestamp": 1717920000.0,
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "sample-event-id",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::123456789012:role/logstream-lambda-role",
            "awsRegion": "ap-southeast-1",
            "eventSourceARN": "arn:aws:kinesis:ap-southeast-1:123456789012:stream/logstream-raw",
        }
    ]
}

output = Path(__file__).resolve().parent / "events" / "sample-kinesis-event.json"
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(kinesis_event, indent=2), encoding="utf-8")
print(f"Created {output}")
