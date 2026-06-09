# LogStream — Database Schema

Tài liệu cấu trúc bảng **PostgreSQL** (persistent) và **DynamoDB** (buffer tạm trong lúc transaction đang chạy).

---

## Tổng quan vai trò

| Store | Vai trò | Khi nào ghi |
|---|---|---|
| **DynamoDB** | Buffer tạm: metadata + log chunks theo `transactionId` | Trong lúc transaction `RUNNING` |
| **PostgreSQL** | Source of truth: flow diagram, step metadata | Sau khi transaction kết thúc (flush từ DynamoDB) |
| **S3** | Full log archive theo `traceId` / step | Sau khi transaction kết thúc (flush từ DynamoDB) |

---

## PostgreSQL

### ERD

```
transactions (1) ──< (N) transaction_steps
```

### Bảng `transactions`

Lưu trạng thái tổng thể của một `transactionId`.

```sql
CREATE TABLE transactions (
    transaction_id    VARCHAR(64)   PRIMARY KEY,
    status            VARCHAR(20)   NOT NULL DEFAULT 'RUNNING',
    current_step      VARCHAR(64),
    started_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    completed_at      TIMESTAMPTZ,
    last_updated_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    issue_detected    BOOLEAN       NOT NULL DEFAULT FALSE,
    issue_type        VARCHAR(50),
    metadata          JSONB,

    CONSTRAINT chk_transactions_status
        CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'TIMEOUT'))
);

CREATE INDEX idx_transactions_status
    ON transactions (status);

CREATE INDEX idx_transactions_started_at
    ON transactions (started_at DESC);

CREATE INDEX idx_transactions_issue
    ON transactions (issue_detected)
    WHERE issue_detected = TRUE;
```

| Cột | Kiểu | Mô tả |
|---|---|---|
| `transaction_id` | VARCHAR(64) | ID giao dịch (PK) |
| `status` | VARCHAR(20) | `RUNNING` \| `COMPLETED` \| `FAILED` \| `TIMEOUT` |
| `current_step` | VARCHAR(64) | Step đang chạy / step cuối |
| `started_at` | TIMESTAMPTZ | Thời điểm bắt đầu transaction |
| `completed_at` | TIMESTAMPTZ | Thời điểm kết thúc (null nếu đang chạy) |
| `last_updated_at` | TIMESTAMPTZ | Cập nhật lần cuối |
| `issue_detected` | BOOLEAN | Có issue từ Detection Engine |
| `issue_type` | VARCHAR(50) | Loại issue: `ERROR_PATTERN`, `TIMEOUT`, `FRAUD`, ... |
| `metadata` | JSONB | Dữ liệu mở rộng (service origin, userId, ...) |

---

### Bảng `transaction_steps`

Lưu từng step trong flow diagram. Mỗi step gắn với một `traceId`.

```sql
CREATE TABLE transaction_steps (
    id               BIGSERIAL     PRIMARY KEY,
    transaction_id   VARCHAR(64)   NOT NULL
                         REFERENCES transactions (transaction_id)
                         ON DELETE CASCADE,
    step_name        VARCHAR(64)   NOT NULL,
    step_order       INT           NOT NULL,
    trace_id         VARCHAR(64)   NOT NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'STARTED',
    service_name     VARCHAR(64),
    started_at       TIMESTAMPTZ,
    ended_at         TIMESTAMPTZ,
    duration_ms      INT,
    error_code       VARCHAR(50),
    error_message    TEXT,
    s3_log_key       VARCHAR(512),
    log_line_count   INT,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_transaction_step_name
        UNIQUE (transaction_id, step_name),

    CONSTRAINT uq_transaction_step_order
        UNIQUE (transaction_id, step_order),

    CONSTRAINT uq_trace_id
        UNIQUE (trace_id),

    CONSTRAINT chk_transaction_steps_status
        CHECK (status IN ('STARTED', 'SUCCESS', 'ERROR', 'TIMEOUT', 'SKIPPED'))
);

CREATE INDEX idx_transaction_steps_transaction_id
    ON transaction_steps (transaction_id, step_order);

CREATE INDEX idx_transaction_steps_trace_id
    ON transaction_steps (trace_id);

CREATE INDEX idx_transaction_steps_status
    ON transaction_steps (status)
    WHERE status IN ('ERROR', 'TIMEOUT');
```

| Cột | Kiểu | Mô tả |
|---|---|---|
| `id` | BIGSERIAL | Surrogate key |
| `transaction_id` | VARCHAR(64) | FK → `transactions` |
| `step_name` | VARCHAR(64) | Tên step: `AUTH`, `PAYMENT`, `NOTIFY`, ... |
| `step_order` | INT | Thứ tự step trong flow (1, 2, 3, ...) |
| `trace_id` | VARCHAR(64) | Trace ID của step (unique toàn hệ thống) |
| `status` | VARCHAR(20) | `STARTED` \| `SUCCESS` \| `ERROR` \| `TIMEOUT` \| `SKIPPED` |
| `service_name` | VARCHAR(64) | Microservice xử lý step |
| `started_at` | TIMESTAMPTZ | Step bắt đầu |
| `ended_at` | TIMESTAMPTZ | Step kết thúc |
| `duration_ms` | INT | Thời gian xử lý (ms) |
| `error_code` | VARCHAR(50) | Mã lỗi nếu có |
| `error_message` | TEXT | Mô tả lỗi |
| `s3_log_key` | VARCHAR(512) | S3 path tới full log của step |
| `log_line_count` | INT | Số dòng log đã archive |

---

### Query mẫu — Flow diagram

```sql
-- Lấy flow diagram của 1 transaction
SELECT
    step_order,
    step_name,
    trace_id,
    status,
    service_name,
    started_at,
    ended_at,
    duration_ms,
    error_code,
    s3_log_key
FROM transaction_steps
WHERE transaction_id = 'TXN-001'
ORDER BY step_order;
```

---

## DynamoDB

### Table: `LogStreamTransactions`

Single-table design. Buffer tạm trong lúc transaction đang chạy. Xóa sau khi flush lên S3 + PostgreSQL.

#### Key schema

| Key | Pattern | Ví dụ |
|---|---|---|
| **PK** (Partition Key) | `TXN#<transactionId>` | `TXN#TXN-001` |
| **SK** (Sort Key) | `<type>#<discriminator>` | `META`, `STEP#02#PAYMENT`, `LOG#trace-bbb#0001` |

#### Table definition

```json
{
  "TableName": "LogStreamTransactions",
  "BillingMode": "PAY_PER_REQUEST",
  "KeySchema": [
    { "AttributeName": "pk", "KeyType": "HASH" },
    { "AttributeName": "sk", "KeyType": "RANGE" }
  ],
  "AttributeDefinitions": [
    { "AttributeName": "pk",     "AttributeType": "S" },
    { "AttributeName": "sk",     "AttributeType": "S" },
    { "AttributeName": "gsi1pk", "AttributeType": "S" },
    { "AttributeName": "gsi1sk", "AttributeType": "S" },
    { "AttributeName": "gsi2pk", "AttributeType": "S" },
    { "AttributeName": "gsi2sk", "AttributeType": "S" }
  ],
  "GlobalSecondaryIndexes": [
    {
      "IndexName": "GSI1-StatusIndex",
      "KeySchema": [
        { "AttributeName": "gsi1pk", "KeyType": "HASH" },
        { "AttributeName": "gsi1sk", "KeyType": "RANGE" }
      ],
      "Projection": { "ProjectionType": "KEYS_ONLY" }
    },
    {
      "IndexName": "GSI2-TraceIndex",
      "KeySchema": [
        { "AttributeName": "gsi2pk", "KeyType": "HASH" },
        { "AttributeName": "gsi2sk", "KeyType": "RANGE" }
      ],
      "Projection": { "ProjectionType": "KEYS_ONLY" }
    }
  ],
  "TimeToLiveSpecification": {
    "AttributeName": "ttl",
    "Enabled": true
  },
  "StreamSpecification": {
    "StreamEnabled": true,
    "StreamViewType": "OLD_IMAGE"
  }
}
```

---

### Item type 1: `META`

Một item / transaction. Trạng thái tổng thể.

```
PK:  TXN#TXN-001
SK:  META
```

| Attribute | Type | Mô tả |
|---|---|---|
| `transactionId` | S | ID giao dịch |
| `status` | S | `RUNNING` \| `COMPLETED` \| `FAILED` \| `TIMEOUT` |
| `currentStep` | S | Step hiện tại |
| `stepCount` | N | Số step đã ghi nhận |
| `startedAt` | S | ISO 8601 |
| `lastUpdatedAt` | S | ISO 8601 — dùng cho timeout detection |
| `completedAt` | S \| null | ISO 8601 |
| `ttl` | N | Epoch seconds — auto-delete (vd: +24h) |
| `gsi1pk` | S | `STATUS#RUNNING` — **chỉ set khi RUNNING** (sparse GSI) |
| `gsi1sk` | S | `lastUpdatedAt` — dùng range query timeout |
| `metadata` | M | Map mở rộng |

```json
{
  "pk": "TXN#TXN-001",
  "sk": "META",
  "transactionId": "TXN-001",
  "status": "RUNNING",
  "currentStep": "PAYMENT",
  "stepCount": 2,
  "startedAt": "2026-06-09T10:00:00.000Z",
  "lastUpdatedAt": "2026-06-09T10:00:05.123Z",
  "completedAt": null,
  "ttl": 1749547200,
  "gsi1pk": "STATUS#RUNNING",
  "gsi1sk": "2026-06-09T10:00:05.123Z",
  "metadata": {
    "originService": "order-svc"
  }
}
```

---

### Item type 2: `STEP`

Một item / step. Metadata step, **không** chứa full log.

```
PK:  TXN#TXN-001
SK:  STEP#<order>#<stepName>     ← zero-pad order: STEP#02#PAYMENT
```

| Attribute | Type | Mô tả |
|---|---|---|
| `stepName` | S | Tên step |
| `stepOrder` | N | Thứ tự (1, 2, 3, ...) |
| `traceId` | S | Trace ID của step |
| `status` | S | `STARTED` \| `SUCCESS` \| `ERROR` \| `TIMEOUT` \| `SKIPPED` |
| `serviceName` | S | Microservice |
| `startedAt` | S | ISO 8601 |
| `endedAt` | S \| null | ISO 8601 |
| `durationMs` | N \| null | Thời gian xử lý |
| `errorCode` | S \| null | Mã lỗi |
| `errorMessage` | S \| null | Mô tả lỗi |
| `logChunkCount` | N | Số chunk LOG đã buffer |
| `gsi2pk` | S | `TRACE#<traceId>` |
| `gsi2sk` | S | `TXN#<transactionId>` |

```json
{
  "pk": "TXN#TXN-001",
  "sk": "STEP#02#PAYMENT",
  "stepName": "PAYMENT",
  "stepOrder": 2,
  "traceId": "trace-bbb",
  "status": "STARTED",
  "serviceName": "payment-svc",
  "startedAt": "2026-06-09T10:00:03.000Z",
  "endedAt": null,
  "durationMs": null,
  "errorCode": null,
  "errorMessage": null,
  "logChunkCount": 1,
  "gsi2pk": "TRACE#trace-bbb",
  "gsi2sk": "TXN#TXN-001"
}
```

---

### Item type 3: `LOG`

Buffer log tạm theo chunk. Mỗi chunk tối đa ~300 KB (giới hạn item DynamoDB 400 KB).

```
PK:  TXN#TXN-001
SK:  LOG#<traceId>#<chunkSeq>    ← LOG#trace-bbb#0001
```

| Attribute | Type | Mô tả |
|---|---|---|
| `traceId` | S | Trace ID |
| `stepName` | S | Step tương ứng |
| `chunkSeq` | N | Số thứ tự chunk (1, 2, 3, ...) |
| `logLines` | L | List các log event (JSON object) |
| `lineCount` | N | Số dòng trong chunk |
| `byteSize` | N | Kích thước ước tính (bytes) |
| `createdAt` | S | ISO 8601 |

```json
{
  "pk": "TXN#TXN-001",
  "sk": "LOG#trace-bbb#0001",
  "traceId": "trace-bbb",
  "stepName": "PAYMENT",
  "chunkSeq": 1,
  "logLines": [
    {
      "timestamp": "2026-06-09T10:00:03.100Z",
      "level": "INFO",
      "message": "Payment request received",
      "transactionId": "TXN-001",
      "traceId": "trace-bbb",
      "step": "PAYMENT"
    },
    {
      "timestamp": "2026-06-09T10:00:03.500Z",
      "level": "ERROR",
      "message": "Payment declined",
      "errorCode": "PAYMENT_DECLINED"
    }
  ],
  "lineCount": 2,
  "byteSize": 512,
  "createdAt": "2026-06-09T10:00:03.600Z"
}
```

---

### Cấu trúc item theo transaction

```
TXN#TXN-001
├── META
├── STEP#01#AUTH
├── STEP#02#PAYMENT
├── LOG#trace-aaa#0001
├── LOG#trace-bbb#0001
└── LOG#trace-bbb#0002          ← chunk thêm nếu log dài
```

---

### GSI

#### GSI1 — `GSI1-StatusIndex` (timeout detection)

| Key | Value |
|---|---|
| `gsi1pk` | `STATUS#RUNNING` |
| `gsi1sk` | `lastUpdatedAt` |

```python
# Query transaction RUNNING quá 30 giây
table.query(
    IndexName='GSI1-StatusIndex',
    KeyConditionExpression='gsi1pk = :status AND gsi1sk < :threshold',
    ExpressionAttributeValues={
        ':status': 'STATUS#RUNNING',
        ':threshold': '2026-06-09T10:00:00.000Z'
    }
)
```

> **Sparse GSI:** Khi transaction `COMPLETED` / `FAILED`, xóa `gsi1pk` và `gsi1sk` khỏi item META.

#### GSI2 — `GSI2-TraceIndex` (tra cứu ngược theo traceId)

| Key | Value |
|---|---|
| `gsi2pk` | `TRACE#<traceId>` |
| `gsi2sk` | `TXN#<transactionId>` |

```python
# Tìm transactionId từ traceId
table.query(
    IndexName='GSI2-TraceIndex',
    KeyConditionExpression='gsi2pk = :trace',
    ExpressionAttributeValues={
        ':trace': 'TRACE#trace-bbb'
    }
)
```

---

### Access patterns

| # | Hành động | Operation |
|---|---|---|
| 1 | Log event mới | `UpdateItem` META + `PutItem`/`UpdateItem` STEP + append `LOG` chunk |
| 2 | Đọc state transaction | `GetItem` PK=`TXN#id`, SK=`META` |
| 3 | Đọc tất cả steps | `Query` PK=`TXN#id`, SK `begins_with` `STEP#` |
| 4 | Đọc log buffer 1 step | `Query` PK=`TXN#id`, SK `begins_with` `LOG#trace-xxx#` |
| 5 | Timeout sweep | `Query` GSI1: `STATUS#RUNNING` + `gsi1sk < threshold` |
| 6 | Tra cứu theo traceId | `Query` GSI2: `TRACE#<traceId>` |
| 7 | Transaction kết thúc | `Query` toàn PK → flush S3 + PostgreSQL → `BatchWriteItem` delete |
| 8 | Cleanup orphan | TTL auto-delete; DynamoDB Streams trigger flush trước khi xóa |

---

### Flush flow (DynamoDB → S3 + PostgreSQL)

```
1. Query PK = TXN#<transactionId>  (lấy META + STEP + LOG)
2. Group LOG items theo traceId
3. Với mỗi traceId:
     → Ghi S3: s3://logstream-archive/year=.../transactionId=.../traceId=.../part-0001.jsonl.gz
4. UPSERT PostgreSQL:
     → transactions (status, completed_at, ...)
     → transaction_steps (step metadata, s3_log_key, log_line_count)
5. BatchDelete tất cả items PK = TXN#<transactionId>
```

---

## Mapping DynamoDB → PostgreSQL + S3

| DynamoDB | PostgreSQL | S3 |
|---|---|---|
| `META` | `transactions` | — |
| `STEP#xx#name` | `transaction_steps` (metadata) | — |
| `LOG#traceId#nnn` (gộp theo traceId) | `transaction_steps.s3_log_key` | `.../transactionId/traceId/*.jsonl.gz` |

---

## Ràng buộc & lưu ý

| Ràng buộc | Giải pháp |
|---|---|
| DynamoDB item max 400 KB | Chunk log ~300 KB/item, tạo `LOG#...#0002` khi đầy |
| Transaction orphan | TTL 24h + DynamoDB Streams flush trước khi xóa |
| Hot partition | PK = `TXN#<transactionId>` — phân tán tốt với UUID |
| Ghi log liên tục | Buffer in-memory 50–100 dòng hoặc 5s rồi batch write |
| Step transition atomic | `TransactWriteItems` cập nhật META + STEP cùng lúc |
