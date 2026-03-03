# Mini Data Platform — Implementation-Ready Blueprint

> **Use case:** Sales CSV uploads (orders) with fields: `order_id`, `order_date`, `customer_id`, `region`, `product`, `quantity`, `unit_price`.

---

## 1. Architecture

### 1.1 Component Responsibilities

| Component | Role | Port |
|-----------|------|------|
| **MinIO** | S3-compatible object store — CSV landing zone, processed archive, quarantine, and error reports | `9000` (API), `9001` (Console) |
| **Apache Airflow** | Orchestrates the ETL DAG (discover → validate → transform → load → audit → move) | `8080` |
| **PostgreSQL** | Curated analytics storage — fact table, summary table, audit table | `5432` |
| **Metabase** | Self-service BI dashboards connected to PostgreSQL | `3000` |

### 1.2 Data Flow

```
                        ┌───────────────────────────────────────────────────┐
                        │                    MinIO                          │
                        │                                                   │
  CSV Upload ──────────►│  landing/sales/       ──── success ──►  processed/sales/  │
                        │       │                                           │
                        │       │               ──── failure ──►  quarantine/sales/  │
                        │       │                               + errors/<run_id>.json│
                        └───────┼───────────────────────────────────────────┘
                                │
                    ┌───────────▼─────────────┐
                    │     Airflow DAG          │
                    │                         │
                    │  discover_files          │
                    │       │                  │
                    │  validate_schema         │
                    │       │                  │
                    │  transform_data          │
                    │       │                  │
                    │  load_to_postgres        │
                    │       │                  │
                    │  write_audit_row         │
                    │       │                  │
                    │  move_file               │
                    └───────┼─────────────────┘
                            │
                    ┌───────▼─────────────────┐
                    │     PostgreSQL            │
                    │                          │
                    │  curated.fact_orders      │
                    │  curated.summary_daily    │
                    │  audit.etl_audit_runs     │
                    └───────┼──────────────────┘
                            │
                    ┌───────▼─────────────────┐
                    │     Metabase              │
                    │  (reads curated.* views)  │
                    └──────────────────────────┘
```

### 1.3 Failure Paths

| Failure Scenario | Behaviour |
|---|---|
| **Schema validation fails** (bad types, nulls, constraint violations) | File is copied to `quarantine/sales/<filename>`. A JSON error report is written to `errors/<run_id>.json` in MinIO. Audit row written with `status = 'FAILED'`. Pipeline does **not** halt subsequent files. |
| **Transformation error** (unexpected data during cleaning) | Same quarantine flow. Error detail captured in audit `error_message` column. |
| **Postgres load failure** (connection timeout, constraint violation) | Airflow task retries 3× with exponential backoff (30 s, 60 s, 120 s). If all retries fail → quarantine + error report. |
| **MinIO unreachable** | Airflow task retries 3× exponential backoff. DAG run marked as failed; alert via Airflow email/Slack callback. |
| **Duplicate file re-run (idempotency)** | UPSERT (`INSERT … ON CONFLICT DO UPDATE`) keyed on `order_id` ensures safe reruns. Audit table records every run regardless. |

---

## 2. Repository Structure

```
mini-data-platform/
├── docker-compose.yml              # All 4 services + shared network
├── .env.example                     # Template for secrets (never commit real .env)
├── .gitignore
├── README.md
│
├── airflow/
│   ├── Dockerfile                   # Extends apache/airflow with pip deps
│   ├── requirements.txt             # pandas, pandera, minio, psycopg2-binary
│   ├── dags/
│   │   └── sales_etl_dag.py         # Main DAG definition
│   ├── plugins/
│   │   ├── etl/
│   │   │   ├── __init__.py
│   │   │   ├── extract.py           # MinIO discover + download
│   │   │   ├── validate.py          # Pandera schema validation
│   │   │   ├── transform.py         # Cleaning, type casting, derived columns
│   │   │   ├── load.py              # Postgres UPSERT logic
│   │   │   ├── audit.py             # Write audit rows
│   │   │   └── file_mover.py        # Move to processed/ or quarantine/
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── minio_client.py      # MinIO connection helper
│   │       ├── pg_client.py         # Postgres connection helper
│   │       └── logger.py            # Structured JSON logging helper
│   └── config/
│       └── airflow.cfg.override     # Key config overrides (optional)
│
├── contracts/
│   └── sales_contract.yml           # Data contract for sales CSV
│
├── db/
│   ├── init.sql                     # DDL: schemas, tables, indexes, roles
│   └── seed.sql                     # Optional: seed data for dev
│
├── metabase/
│   └── metabase.db/                 # (gitignored) Metabase H2 database
│
├── minio/
│   └── bootstrap.sh                 # Creates buckets on first run (entrypoint script)
│
├── scripts/
│   ├── generate_sample_data.py      # Generates fake sales CSVs
│   └── upload_to_minio.py           # Uploads CSVs to MinIO landing/
│
├── tests/
│   ├── unit/
│   │   ├── test_validate.py
│   │   ├── test_transform.py
│   │   └── test_load.py
│   ├── integration/
│   │   └── test_end_to_end.py       # Full flow: CSV → MinIO → Airflow → PG assertion
│   └── conftest.py                  # Shared fixtures
│
├── docs/
│   ├── architecture.md              # Architecture diagram + notes
│   ├── data_dictionary.md           # Column-level docs
│   ├── retention_policy.md          # Data retention / access notes
│   └── screenshots/                 # Dashboard screenshots for README
│
└── .github/
    └── workflows/
        ├── ci.yml                   # Lint, unit test, docker build
        └── cd.yml                   # Deploy via self-hosted runner
```

---

## 3. Data Contract

### `contracts/sales_contract.yml`

```yaml
contract:
  name: sales_orders
  version: "1.0"
  owner: data-engineering
  description: "Inbound sales order CSV files from upstream systems"

source:
  format: csv
  encoding: utf-8
  delimiter: ","
  has_header: true
  landing_path: "landing/sales/"

columns:
  - name: order_id
    type: string
    required: true
    unique: true
    pattern: "^ORD-[0-9]{6,10}$"
    description: "Unique order identifier, format: ORD-NNNNNN"

  - name: order_date
    type: date
    required: true
    format: "%Y-%m-%d"
    constraints:
      min: "2020-01-01"
      max: "today+1"        # no future dates beyond tomorrow
    description: "Date the order was placed"

  - name: customer_id
    type: string
    required: true
    pattern: "^CUST-[0-9]{4,8}$"
    description: "Customer identifier"

  - name: region
    type: string
    required: true
    allowed_values:
      - "North America"
      - "Europe"
      - "Asia Pacific"
      - "Latin America"
      - "Middle East & Africa"
    description: "Sales region"

  - name: product
    type: string
    required: true
    min_length: 2
    max_length: 100
    description: "Product name"

  - name: quantity
    type: integer
    required: true
    constraints:
      min: 1
      max: 100000
    description: "Number of units ordered"

  - name: unit_price
    type: float
    required: true
    constraints:
      min: 0.01
      max: 999999.99
    description: "Price per unit in USD"

business_rules:
  - rule: "total_amount = quantity * unit_price"
    description: "Derived field, computed during transformation"
  - rule: "no duplicate order_id within a single file"
  - rule: "order_date must not be more than 1 day in the future"

examples:
  valid:
    - order_id: "ORD-000001"
      order_date: "2025-06-15"
      customer_id: "CUST-1234"
      region: "Europe"
      product: "Widget Pro"
      quantity: 10
      unit_price: 29.99

    - order_id: "ORD-000002"
      order_date: "2025-06-16"
      customer_id: "CUST-5678"
      region: "Asia Pacific"
      product: "Gadget X"
      quantity: 1
      unit_price: 499.00

  invalid:
    - order_id: "12345"           # ← violates pattern
      order_date: "2025-06-15"
      customer_id: "CUST-1234"
      region: "Europe"
      product: "Widget Pro"
      quantity: 10
      unit_price: 29.99
      reason: "order_id does not match ORD-NNNNNN pattern"

    - order_id: "ORD-000003"
      order_date: "2030-01-01"    # ← future date too far out
      customer_id: "CUST-1234"
      region: "Antarctica"        # ← not in allowed_values
      product: "W"                # ← min_length = 2 violated
      quantity: -5                # ← below min
      unit_price: 0.00            # ← below min
      reason: "Multiple violations: future date, invalid region, short product name, negative qty, zero price"
```

---

## 4. PostgreSQL Schema

### DDL

```sql
-- ============================================================
-- SCHEMAS
-- ============================================================
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================================
-- CURATED: Fact table (orders)
-- ============================================================
CREATE TABLE curated.fact_orders (
    order_id        VARCHAR(20)    PRIMARY KEY,
    order_date      DATE           NOT NULL,
    customer_id     VARCHAR(20)    NOT NULL,
    region          VARCHAR(50)    NOT NULL,
    product         VARCHAR(100)   NOT NULL,
    quantity        INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(10,2)  NOT NULL CHECK (unit_price > 0),
    total_amount    NUMERIC(12,2)  NOT NULL GENERATED ALWAYS AS (quantity * unit_price) STORED,
    loaded_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    source_file     VARCHAR(255)   NOT NULL
);

-- Indexes for common dashboard queries
CREATE INDEX idx_fact_orders_date   ON curated.fact_orders (order_date);
CREATE INDEX idx_fact_orders_region ON curated.fact_orders (region);
CREATE INDEX idx_fact_orders_cust   ON curated.fact_orders (customer_id);

-- ============================================================
-- CURATED: Summary table (daily aggregates for dashboards)
-- ============================================================
CREATE TABLE curated.summary_daily_sales (
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
CREATE TABLE audit.etl_audit_runs (
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

CREATE INDEX idx_audit_status  ON audit.etl_audit_runs (status);
CREATE INDEX idx_audit_started ON audit.etl_audit_runs (started_at);

-- ============================================================
-- UPSERT STRATEGY (used in load.py)
-- ============================================================
-- INSERT INTO curated.fact_orders (order_id, order_date, customer_id, region,
--     product, quantity, unit_price, source_file)
-- VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
-- ON CONFLICT (order_id) DO UPDATE SET
--     order_date   = EXCLUDED.order_date,
--     customer_id  = EXCLUDED.customer_id,
--     region       = EXCLUDED.region,
--     product      = EXCLUDED.product,
--     quantity     = EXCLUDED.quantity,
--     unit_price   = EXCLUDED.unit_price,
--     source_file  = EXCLUDED.source_file,
--     loaded_at    = NOW();

-- Summary table is fully refreshed per (date, region) after each load:
-- DELETE then INSERT for the affected dates/regions in the batch.

-- ============================================================
-- ROLES
-- ============================================================
-- etl_writer: used by Airflow
CREATE ROLE etl_writer WITH LOGIN PASSWORD '${ETL_WRITER_PASSWORD}';
GRANT USAGE, CREATE ON SCHEMA curated, audit TO etl_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA curated TO etl_writer;
GRANT SELECT, INSERT, UPDATE         ON ALL TABLES IN SCHEMA audit   TO etl_writer;

-- bi_reader: used by Metabase (read-only)
CREATE ROLE bi_reader WITH LOGIN PASSWORD '${BI_READER_PASSWORD}';
GRANT USAGE  ON SCHEMA curated TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA curated TO bi_reader;
GRANT USAGE  ON SCHEMA audit   TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA audit   TO bi_reader;

-- Ensure future tables inherit grants
ALTER DEFAULT PRIVILEGES IN SCHEMA curated GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA curated GRANT ALL    ON TABLES TO etl_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit   GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit   GRANT SELECT, INSERT, UPDATE ON TABLES TO etl_writer;
```

---

## 5. Pipeline Design (Airflow DAG)

### 5.1 DAG Definition

```
DAG ID:        sales_etl
Schedule:      */15 * * * *   (every 15 minutes)
Catchup:       False
Max active:    1              (prevents overlapping runs)
Tags:          ["etl", "sales"]
```

### 5.2 Task Breakdown

```
discover_files  →  validate_schema  →  transform_data  →  load_to_postgres  →  refresh_summary  →  write_audit_row  →  move_file
```

| Task | Module | Description |
|------|--------|-------------|
| `discover_files` | `extract.py` | List objects in `landing/sales/` via MinIO client. Push list of keys to XCom. If no files → short-circuit (skip downstream). |
| `validate_schema` | `validate.py` | Download each CSV. Run Pandera schema checks. Split into `valid_df` and `invalid_rows`. Push to XCom or temp file path. If 100% invalid → mark as quarantine, skip transform. |
| `transform_data` | `transform.py` | Strip whitespace, cast types, compute `total_amount`, normalize region casing, de-duplicate by `order_id` (keep last). |
| `load_to_postgres` | `load.py` | Batch UPSERT into `curated.fact_orders` using `psycopg2` `execute_values` with `ON CONFLICT`. Track `rows_loaded`. |
| `refresh_summary` | `load.py` | Re-aggregate `curated.summary_daily_sales` for affected `(date, region)` pairs. Use `DELETE` + `INSERT` pattern. |
| `write_audit_row` | `audit.py` | Insert row into `audit.etl_audit_runs` with `rows_in`, `rows_valid`, `rows_loaded`, `status`, `error_message`, timestamps. |
| `move_file` | `file_mover.py` | On **success**: copy to `processed/sales/<YYYY-MM-DD>/<filename>`, delete from `landing/`. On **failure**: copy to `quarantine/sales/<filename>`, write `errors/<run_id>.json` with error details, delete from `landing/`. |

### 5.3 Retry & Backoff Rules

```python
default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": alert_on_failure,   # sends Slack/email
}
```

### 5.4 Logging Requirements

All ETL modules use the structured logger from `utils/logger.py`:

```python
import json, logging

class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "task": getattr(record, "task_name", "unknown"),
            "dag_run": getattr(record, "dag_run_id", "unknown"),
            "file_key": getattr(record, "file_key", ""),
            "message": record.getMessage(),
        }
        if record.exc_info:
            log["exception"] = self.formatException(record.exc_info)
        return json.dumps(log)
```

Every log line includes: `timestamp`, `level`, `task_name`, `dag_run_id`, `file_key`, and `message`.

### 5.5 File Movement & Error Reports

**Success path:**
```
landing/sales/orders_20250615.csv
  → processed/sales/2025-06-15/orders_20250615.csv
```

**Failure path:**
```
landing/sales/orders_bad.csv
  → quarantine/sales/orders_bad.csv
  → errors/a1b2c3d4-run-id.json
```

**Error report format (`errors/<run_id>.json`):**
```json
{
  "run_id": "a1b2c3d4-...",
  "file_key": "landing/sales/orders_bad.csv",
  "timestamp": "2025-06-15T10:30:00Z",
  "status": "FAILED",
  "rows_in": 150,
  "rows_valid": 120,
  "rows_rejected": 30,
  "errors": [
    {"row": 5, "column": "quantity", "error": "Value -3 below minimum 1"},
    {"row": 12, "column": "region", "error": "'Antarctica' not in allowed_values"}
  ]
}
```

---

## 6. Data Quality Strategy

### 6.1 Tool Choice: **Pandera**

| | Pandera | Great Expectations |
|-|---------|--------------------|
| **Setup** | `pip install pandera`, pure Python | Heavy install, stores/checkpoints config |
| **Integration** | DataFrame-native, fits in-DAG task | Separate CLI / execution layer |
| **Learning curve** | Low — decorators + schema classes | Moderate — YAML suites, docs sites |
| **Fit for mini platform** | Yes — Lightweight, no extra infra | No — Overkill for a single-dataset project |

**Verdict:** Pandera — minimal overhead, integrates directly into the Airflow task Python code, and produces clear error messages per-row.

### 6.2 Pandera Schema Definition

```python
import pandera as pa
from pandera import Column, Check, Index
from datetime import date, timedelta

sales_schema = pa.DataFrameSchema(
    columns={
        "order_id":    Column(str, [
            Check.str_matches(r"^ORD-\d{6,10}$"),
        ], nullable=False, unique=True),

        "order_date":  Column("datetime64[ns]", [
            Check.greater_than_or_equal_to(pd.Timestamp("2020-01-01")),
            Check.less_than_or_equal_to(pd.Timestamp(date.today() + timedelta(days=1))),
        ], nullable=False),

        "customer_id": Column(str, [
            Check.str_matches(r"^CUST-\d{4,8}$"),
        ], nullable=False),

        "region":      Column(str, [
            Check.isin([
                "North America", "Europe", "Asia Pacific",
                "Latin America", "Middle East & Africa"
            ]),
        ], nullable=False),

        "product":     Column(str, [
            Check.str_length(min_value=2, max_value=100),
        ], nullable=False),

        "quantity":    Column(int, [
            Check.greater_than(0),
            Check.less_than_or_equal_to(100000),
        ], nullable=False),

        "unit_price":  Column(float, [
            Check.greater_than(0),
            Check.less_than_or_equal_to(999999.99),
        ], nullable=False),
    },
    coerce=True,
    strict=True,          # reject extra columns
    name="SalesOrderSchema",
)
```

### 6.3 Checks Implemented

| Check Type | Columns | Details |
|------------|---------|---------|
| **Type coercion** | All | `coerce=True` — cast before validating |
| **Null check** | All | `nullable=False` on every column |
| **Regex pattern** | `order_id`, `customer_id` | Format enforcement |
| **Allowed values** | `region` | Closed set of 5 regions |
| **Range check** | `quantity`, `unit_price` | Min/max boundaries |
| **Date sanity** | `order_date` | Not before 2020, not more than 1 day in future |
| **String length** | `product` | 2–100 characters |
| **Uniqueness** | `order_id` | No duplicates within file |
| **Strict mode** | Entire DataFrame | Reject unexpected columns |

---

## 7. Security & Governance

### 7.1 PostgreSQL Roles

(See DDL in Section 4 — `etl_writer` and `bi_reader` roles with principle of least privilege.)

- **`etl_writer`** — Airflow uses this. Can SELECT/INSERT/UPDATE/DELETE on `curated.*` and SELECT/INSERT/UPDATE on `audit.*`.
- **`bi_reader`** — Metabase uses this. SELECT-only on `curated.*` and `audit.*`.

### 7.2 Secrets Handling

**`.env.example`** (committed to repo):
```env
# PostgreSQL
POSTGRES_DB=dataplatform
POSTGRES_USER=admin
POSTGRES_PASSWORD=changeme

# Role passwords
ETL_WRITER_PASSWORD=changeme
BI_READER_PASSWORD=changeme

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=changeme

# Airflow
AIRFLOW__CORE__FERNET_KEY=changeme
AIRFLOW__WEBSERVER__SECRET_KEY=changeme
AIRFLOW_ADMIN_PASSWORD=changeme
```

- **`.env`** is in `.gitignore` and never committed.
- **GitHub Actions:** All secrets stored in **GitHub Settings → Secrets → Actions**. Referenced as `${{ secrets.POSTGRES_PASSWORD }}`, etc.
- **Docker Compose:** Uses `env_file: .env` to inject secrets into containers.

### 7.3 Retention Policy & Access Notes

Document in `docs/retention_policy.md`:

| Data | Location | Retention | Notes |
|------|----------|-----------|-------|
| Raw CSVs | MinIO `processed/` | 90 days | Auto-expire via MinIO lifecycle rule |
| Quarantined CSVs | MinIO `quarantine/` | 30 days | Review + reprocess or discard |
| Error reports | MinIO `errors/` | 30 days | Delete after investigation |
| `fact_orders` | PostgreSQL | Indefinite | Partitioning by year if > 10 M rows |
| `summary_daily_sales` | PostgreSQL | Indefinite | Small table, no concern |
| `etl_audit_runs` | PostgreSQL | 1 year | Purge runs older than 1 year via scheduled job |
| Airflow logs | Container volume | 14 days | Clean via Airflow log cleanup DAG |

---

## 8. Testing Plan

### 8.1 Unit Tests

| Test File | Function Under Test | Test Cases |
|-----------|-------------------|------------|
| `test_validate.py` | `validate_dataframe(df)` | Valid DataFrame passes. Missing `order_id` -> `SchemaError`. Negative `quantity` -> `SchemaError`. Future date (2030) -> `SchemaError`. Invalid region -> `SchemaError`. Extra column -> `SchemaError` (strict mode). |
| `test_transform.py` | `transform_dataframe(df)` | Whitespace stripped from strings. `total_amount` computed correctly. Duplicates by `order_id` de-duplicated (keep last). Region casing normalized. Empty DataFrame returns empty. |
| `test_load.py` | `upsert_orders(conn, df)` | Inserts rows into empty table. Second call with same `order_id` updates (idempotent). `rows_loaded` count matches DataFrame length. Empty DataFrame -> 0 rows loaded (no error). |
| `test_load.py` | `refresh_summary(conn, dates, regions)` | Summary aggregates match raw fact rows. Re-run produces identical results (idempotent). |

**Run command:** `pytest tests/unit/ -v --tb=short`

### 8.2 Integration Test

**File:** `tests/integration/test_end_to_end.py`

**Flow:**
1. Generate a known CSV (5 valid rows, 2 invalid rows) using `generate_sample_data.py`.
2. Upload to MinIO `landing/sales/` using `upload_to_minio.py`.
3. Trigger the Airflow DAG via REST API (`POST /api/v1/dags/sales_etl/dagRuns`).
4. Poll DAG run status until complete (timeout: 120 s).
5. **Assert:**
   - `curated.fact_orders` contains exactly 5 rows with expected `order_id` values.
   - `curated.summary_daily_sales` has correct aggregate totals.
   - `audit.etl_audit_runs` has 1 row with `status = 'SUCCESS'`, `rows_in = 7`, `rows_valid = 5`, `rows_loaded = 5`.
   - MinIO `processed/sales/` contains the original CSV.
   - MinIO `landing/sales/` is empty.

**Run command:** `pytest tests/integration/ -v --tb=short` (requires `docker compose up -d` first).

---

## 9. GitHub Actions Plan

### 9.1 CI Pipeline (`.github/workflows/ci.yml`)

```yaml
name: CI
on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install ruff
      - run: ruff check airflow/ scripts/ tests/

  unit-tests:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r airflow/requirements.txt pytest
      - run: pytest tests/unit/ -v --tb=short

  docker-build:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - run: docker compose build --no-cache

  integration-test:
    runs-on: ubuntu-latest
    needs: docker-build
    steps:
      - uses: actions/checkout@v4
      - run: cp .env.example .env
      - run: docker compose up -d
      - run: sleep 60              # Wait for all services to initialize
      - run: |
          pip install pytest requests minio psycopg2-binary
          pytest tests/integration/ -v --tb=short
      - run: docker compose down -v
      - if: failure()
        run: docker compose logs > /tmp/compose-logs.txt
      - if: failure()
        uses: actions/upload-artifact@v4
        with: { name: compose-logs, path: /tmp/compose-logs.txt }

  data-flow-validation:
    runs-on: ubuntu-latest
    needs: integration-test
    steps:
      - uses: actions/checkout@v4
      - run: cp .env.example .env
      - run: docker compose up -d
      - run: sleep 60
      - name: Validate data flow
        run: |
          python scripts/generate_sample_data.py --rows 20 --output /tmp/test_sales.csv
          python scripts/upload_to_minio.py --file /tmp/test_sales.csv --bucket landing --prefix sales/
          sleep 120   # Wait for DAG to trigger and complete
          python -c "
          import psycopg2
          conn = psycopg2.connect('postgresql://etl_writer:changeme@localhost:5432/dataplatform')
          cur = conn.cursor()
          cur.execute('SELECT COUNT(*) FROM curated.fact_orders')
          count = cur.fetchone()[0]
          assert count >= 20, f'Expected >=20 rows, got {count}'
          cur.execute('SELECT COUNT(*) FROM audit.etl_audit_runs WHERE status = \'SUCCESS\'')
          audits = cur.fetchone()[0]
          assert audits >= 1, f'Expected >=1 successful audit run, got {audits}'
          print(f'Data flow validated: {count} orders, {audits} audit runs')
          conn.close()
          "
      - run: docker compose down -v
```

### 9.2 CD Pipeline (`.github/workflows/cd.yml`)

```yaml
name: CD
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: self-hosted      # Deployed on your server
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - name: Create .env from secrets
        run: |
          echo "POSTGRES_DB=dataplatform"                        > .env
          echo "POSTGRES_USER=${{ secrets.POSTGRES_USER }}"     >> .env
          echo "POSTGRES_PASSWORD=${{ secrets.POSTGRES_PASSWORD }}" >> .env
          echo "ETL_WRITER_PASSWORD=${{ secrets.ETL_WRITER_PASSWORD }}" >> .env
          echo "BI_READER_PASSWORD=${{ secrets.BI_READER_PASSWORD }}" >> .env
          echo "MINIO_ROOT_USER=${{ secrets.MINIO_ROOT_USER }}" >> .env
          echo "MINIO_ROOT_PASSWORD=${{ secrets.MINIO_ROOT_PASSWORD }}" >> .env
          echo "AIRFLOW__CORE__FERNET_KEY=${{ secrets.FERNET_KEY }}" >> .env
          echo "AIRFLOW__WEBSERVER__SECRET_KEY=${{ secrets.WEBSERVER_SECRET }}" >> .env
          echo "AIRFLOW_ADMIN_PASSWORD=${{ secrets.AIRFLOW_ADMIN_PASSWORD }}" >> .env
      - name: Deploy
        run: |
          docker compose pull
          docker compose up -d --build --remove-orphans
          docker compose ps
      - name: Health check
        run: |
          sleep 30
          curl -f http://localhost:8080/health || exit 1     # Airflow
          curl -f http://localhost:9000/minio/health/live || exit 1  # MinIO
          curl -f http://localhost:3000/api/health || exit 1  # Metabase
```

---

## 10. Definition of Done — Acceptance Checklist

### Infrastructure
- [ ] `docker compose up -d` starts all 4 services without errors
- [ ] MinIO console accessible at `localhost:9001`
- [ ] Airflow webserver accessible at `localhost:8080`
- [ ] PostgreSQL accepts connections on `localhost:5432`
- [ ] Metabase accessible at `localhost:3000`

### Data Pipeline
- [ ] Sample CSV generated and uploaded to MinIO `landing/sales/`
- [ ] Airflow DAG `sales_etl` appears in the UI and triggers on schedule
- [ ] Valid rows land in `curated.fact_orders` with correct data
- [ ] `curated.summary_daily_sales` is populated with correct aggregates
- [ ] `audit.etl_audit_runs` records every run with accurate metrics
- [ ] Processed files moved to `processed/sales/<date>/`
- [ ] Invalid files moved to `quarantine/sales/` with error report in `errors/`

### Idempotency
- [ ] Re-running the DAG on the same file does not create duplicates (UPSERT works)
- [ ] Audit table records each run independently

### Data Quality
- [ ] Pandera rejects rows with invalid types, nulls, out-of-range values
- [ ] Invalid rows are captured in error reports, not silently dropped

### Security
- [ ] `etl_writer` role can INSERT/UPDATE but `bi_reader` cannot
- [ ] `.env` is gitignored; `.env.example` is committed
- [ ] No secrets in code or Docker Compose file

### Testing
- [ ] Unit tests pass: `pytest tests/unit/ -v` — all green
- [ ] Integration test passes: end-to-end CSV → MinIO → Airflow → PostgreSQL

### CI/CD
- [ ] GitHub Actions CI runs lint + unit tests + docker build + integration test on PR
- [ ] GitHub Actions CD deploys to self-hosted runner on merge to `main`

### Documentation & Screenshots
- [ ] `README.md` with setup instructions, architecture overview, usage
- [ ] Screenshot: Airflow DAG graph view (successful run) — `docs/screenshots/airflow_dag.png`
- [ ] Screenshot: Metabase dashboard with sales charts — `docs/screenshots/metabase_dashboard.png`
- [ ] Screenshot: MinIO bucket structure — `docs/screenshots/minio_buckets.png`
- [ ] Screenshot: PostgreSQL query results — `docs/screenshots/postgres_data.png`
