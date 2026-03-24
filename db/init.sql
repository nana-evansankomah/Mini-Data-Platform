-- ============================================================
-- SCHEMAS
-- ============================================================
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================================
-- CURATED: Fact table (orders)
-- ============================================================
CREATE TABLE IF NOT EXISTS curated.fact_orders (
    order_id        VARCHAR(20)    PRIMARY KEY,
    order_date      DATE           NOT NULL,
    customer_id     VARCHAR(20)    NOT NULL,
    region          VARCHAR(50)    NOT NULL,
    product         VARCHAR(100)   NOT NULL,
    quantity        INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(10,2)  NOT NULL CHECK (unit_price > 0),
    total_amount    NUMERIC(12,2)  NOT NULL,
    loaded_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    source_file     VARCHAR(255)   NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_date   ON curated.fact_orders (order_date);
CREATE INDEX IF NOT EXISTS idx_fact_orders_region ON curated.fact_orders (region);
CREATE INDEX IF NOT EXISTS idx_fact_orders_cust   ON curated.fact_orders (customer_id);

-- ============================================================
-- CURATED: Summary table (daily aggregates for dashboards)
-- ============================================================
CREATE TABLE IF NOT EXISTS curated.summary_daily_sales (
    summary_date    DATE           NOT NULL,
    region          VARCHAR(50)    NOT NULL,
    total_orders    INTEGER        NOT NULL DEFAULT 0,
    total_quantity  BIGINT         NOT NULL DEFAULT 0,
    total_revenue   NUMERIC(14,2)  NOT NULL DEFAULT 0.00,
    avg_order_value NUMERIC(10,2)  NOT NULL DEFAULT 0.00,
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (summary_date, region)
);

-- ============================================================
-- AUDIT: ETL run tracking
-- ============================================================
CREATE TABLE IF NOT EXISTS audit.etl_audit_runs (
    run_id          UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_run_id      VARCHAR(255),
    file_key        VARCHAR(500)   NOT NULL,
    rows_in         INTEGER        DEFAULT 0,
    rows_valid      INTEGER        DEFAULT 0,
    rows_loaded     INTEGER        DEFAULT 0,
    status          VARCHAR(20)    NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
    error_message   TEXT,
    started_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_audit_status  ON audit.etl_audit_runs (status);
CREATE INDEX IF NOT EXISTS idx_audit_started ON audit.etl_audit_runs (started_at);
