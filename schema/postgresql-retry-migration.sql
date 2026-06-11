-- LogStream — Retry / Attempt Migration
-- Chạy SAU postgresql.sql trên DB đã tồn tại.
--
-- Mục tiêu:
--   - transaction_steps = 1 row / step logic (flow diagram)
--   - step_attempts     = 1 row / lần thử (mỗi attempt có traceId + S3 log riêng)
--   - Hỗ trợ retry job async với traceId mới, idle timeout thay fixed timeout

-- ============================================================
-- 1. step_attempts: mỗi lần thực thi step (kể cả retry)
-- ============================================================
CREATE TABLE IF NOT EXISTS step_attempts (
    id               BIGSERIAL     PRIMARY KEY,
    transaction_id   VARCHAR(64)   NOT NULL
                         REFERENCES transactions (transaction_id)
                         ON DELETE CASCADE,
    step_name        VARCHAR(64)   NOT NULL,
    attempt_number   INT           NOT NULL,
    trace_id         VARCHAR(64)   NOT NULL,
    parent_trace_id  VARCHAR(64),
    status           VARCHAR(20)   NOT NULL DEFAULT 'STARTED',
    service_name     VARCHAR(64),
    is_retry         BOOLEAN       NOT NULL DEFAULT FALSE,
    started_at       TIMESTAMPTZ,
    ended_at         TIMESTAMPTZ,
    duration_ms      INT,
    error_code       VARCHAR(50),
    error_message    TEXT,
    s3_log_key       VARCHAR(512),
    log_line_count   INT,
    metadata         JSONB,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_step_attempt_trace
        UNIQUE (trace_id),

    CONSTRAINT uq_step_attempt_number
        UNIQUE (transaction_id, step_name, attempt_number),

    CONSTRAINT chk_step_attempts_status
        CHECK (status IN ('STARTED', 'SUCCESS', 'ERROR', 'TIMEOUT', 'SKIPPED'))
);

CREATE INDEX IF NOT EXISTS idx_step_attempts_transaction_step
    ON step_attempts (transaction_id, step_name, attempt_number);

CREATE INDEX IF NOT EXISTS idx_step_attempts_parent_trace
    ON step_attempts (parent_trace_id)
    WHERE parent_trace_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_step_attempts_status
    ON step_attempts (status)
    WHERE status IN ('ERROR', 'TIMEOUT');

-- ============================================================
-- 2. transaction_steps: bổ sung cột tổng hợp attempt
-- ============================================================

-- trace_id trên step = traceId của attempt cuối cùng (backward compat Query API)
ALTER TABLE transaction_steps
    ADD COLUMN IF NOT EXISTS attempt_count   INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS latest_trace_id VARCHAR(64);

-- Đồng bộ latest_trace_id từ trace_id hiện có (migration one-time)
UPDATE transaction_steps
SET latest_trace_id = trace_id
WHERE latest_trace_id IS NULL;

-- Cho phép status RETRYING trên step logic
ALTER TABLE transaction_steps
    DROP CONSTRAINT IF EXISTS chk_transaction_steps_status;

ALTER TABLE transaction_steps
    ADD CONSTRAINT chk_transaction_steps_status
        CHECK (status IN ('STARTED', 'RETRYING', 'SUCCESS', 'ERROR', 'TIMEOUT', 'SKIPPED'));

-- trace_id step-level không còn unique toàn hệ thống — unique nằm ở step_attempts
ALTER TABLE transaction_steps
    DROP CONSTRAINT IF EXISTS uq_trace_id;

-- ============================================================
-- 3. Trigger updated_at cho step_attempts
-- ============================================================
CREATE OR REPLACE FUNCTION update_step_attempts_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_step_attempts_updated_at ON step_attempts;

CREATE TRIGGER trg_step_attempts_updated_at
    BEFORE UPDATE ON step_attempts
    FOR EACH ROW
    EXECUTE FUNCTION update_step_attempts_updated_at();

-- ============================================================
-- Sample queries
-- ============================================================

-- Flow diagram (step logic — 1 row / step)
-- SELECT step_order, step_name, status, attempt_count, latest_trace_id, error_code
-- FROM transaction_steps
-- WHERE transaction_id = 'TXN-001'
-- ORDER BY step_order;

-- Drill-down attempts của 1 step
-- SELECT attempt_number, trace_id, parent_trace_id, is_retry, status,
--        started_at, ended_at, s3_log_key, log_line_count
-- FROM step_attempts
-- WHERE transaction_id = 'TXN-001' AND step_name = 'PAYMENT'
-- ORDER BY attempt_number;
