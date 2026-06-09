-- LogStream — PostgreSQL Schema
-- Chạy file này để tạo bảng persistent cho flow diagram transaction.

-- ============================================================
-- transactions: 1 row / transactionId
-- ============================================================
CREATE TABLE IF NOT EXISTS transactions (
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

CREATE INDEX IF NOT EXISTS idx_transactions_status
    ON transactions (status);

CREATE INDEX IF NOT EXISTS idx_transactions_started_at
    ON transactions (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_issue
    ON transactions (issue_detected)
    WHERE issue_detected = TRUE;

-- ============================================================
-- transaction_steps: 1 row / step (mỗi step = 1 traceId)
-- ============================================================
CREATE TABLE IF NOT EXISTS transaction_steps (
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

CREATE INDEX IF NOT EXISTS idx_transaction_steps_transaction_id
    ON transaction_steps (transaction_id, step_order);

CREATE INDEX IF NOT EXISTS idx_transaction_steps_trace_id
    ON transaction_steps (trace_id);

CREATE INDEX IF NOT EXISTS idx_transaction_steps_status
    ON transaction_steps (status)
    WHERE status IN ('ERROR', 'TIMEOUT');

-- ============================================================
-- Trigger: auto-update updated_at on transaction_steps
-- ============================================================
CREATE OR REPLACE FUNCTION update_transaction_steps_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_transaction_steps_updated_at ON transaction_steps;

CREATE TRIGGER trg_transaction_steps_updated_at
    BEFORE UPDATE ON transaction_steps
    FOR EACH ROW
    EXECUTE FUNCTION update_transaction_steps_updated_at();

-- ============================================================
-- Sample query: flow diagram
-- ============================================================
-- SELECT
--     step_order,
--     step_name,
--     trace_id,
--     status,
--     service_name,
--     started_at,
--     ended_at,
--     duration_ms,
--     error_code,
--     s3_log_key
-- FROM transaction_steps
-- WHERE transaction_id = 'TXN-001'
-- ORDER BY step_order;
