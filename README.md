# Mini Data Platform

An industry-grade mini data platform that collects, processes, stores, and visualizes sales data — built entirely with Docker Compose.

## Architecture

| Component | Purpose | Port |
|-----------|---------|------|
| **MinIO** | S3-compatible CSV landing zone & archive | `9000` (API) / `9001` (Console) |
| **Apache Airflow** | ETL orchestration (validate → transform → load) | `8080` |
| **PostgreSQL** | Curated analytics storage & audit trail | `5432` |
| **Metabase** | Self-service BI dashboards | `3000` |

```
CSV Upload → MinIO (landing/) → Airflow ETL → PostgreSQL (curated) → Metabase Dashboards
                                     │
                              ├── processed/
                              ├── quarantine/
                              └── errors/
```

## Quick Start

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/)
- Python 3.11+ (for scripts & tests)

### 1. Clone & Configure

```bash
git clone <your-repo-url>
cd mini-data-platform
cp .env.example .env
# Edit .env with your own passwords (NEVER commit .env)
```

### 2. Start All Services

```bash
docker compose up -d
```

Wait ~60 seconds for all services to initialize, then verify:

| Service | URL |
|---------|-----|
| Airflow | http://localhost:8080 (admin / your-password) |
| MinIO Console | http://localhost:9001 (minioadmin / your-password) |
| Metabase | http://localhost:3000 |
| PostgreSQL | `localhost:5432` (connect via psql or DBeaver) |

If any host ports are already occupied, override them in `.env`:
`POSTGRES_HOST_PORT`, `MINIO_API_PORT`, `MINIO_CONSOLE_PORT`,
`AIRFLOW_HOST_PORT`, `METABASE_HOST_PORT`.

If you change `ETL_WRITER_PASSWORD` or `BI_READER_PASSWORD` after PostgreSQL has already initialized,
recreate the Postgres volume so the init scripts can apply the new role passwords:
`docker compose down -v && docker compose up -d`

### 3. Ingestion (Now Automated in DAG)

The `sales_etl` DAG now runs ingestion as explicit steps before processing:
1. `generate_sample_data`
2. `upload_sample_data`
3. `discover_files`
4. `process_files`
5. `cleanup`

By default, auto-generation is enabled (`ETL_AUTO_GENERATE=true`) and runs on each DAG schedule.
You can tune generated volume with:
- `ETL_AUTO_GENERATE_ROWS` (default `50`)
- `ETL_AUTO_GENERATE_INVALID_ROWS` (default `0`)

If you want to upload your own CSV manually (optional), you can still run:

```bash
pip install minio
python scripts/generate_sample_data.py --rows 50 --output sample_sales.csv
python scripts/upload_to_minio.py --file sample_sales.csv
```

### 4. Watch the Pipeline

The `sales_etl` DAG runs every 15 minutes. To trigger it manually:
1. Open Airflow at http://localhost:8080
2. Find the `sales_etl` DAG
3. Click **Trigger DAG**

### 5. View Dashboards

1. Open Metabase at http://localhost:3000
2. Add PostgreSQL as a Metabase data source: host=`postgres`, port=`5432`, db=`dataplatform`, user=`bi_reader`, password=`BI_READER_PASSWORD` from `.env`
3. Build dashboards from `curated.fact_orders` and `curated.summary_daily_sales`

## Project Structure

```
mini-data-platform/
├── docker-compose.yml          # All 4 services
├── .env.example                # Secrets template
├── airflow/
│   ├── Dockerfile              # Custom Airflow image
│   ├── requirements.txt        # Python deps
│   ├── dags/
│   │   └── sales_etl_dag.py    # Main ETL DAG
│   └── plugins/
│       ├── etl/                # ETL modules (extract, validate, transform, load, audit, file_mover)
│       └── utils/              # Helpers (MinIO client, PG client, structured logger)
├── contracts/
│   └── sales_contract.yml      # Data contract specification
├── db/
│   ├── init.sql                # DDL: schemas, tables, indexes
│   ├── init_airflow_db.sql     # Creates airflow & metabase databases
│   └── init_roles.sh           # Creates/updates etl_writer and bi_reader from env
├── scripts/
│   ├── generate_sample_data.py # Fake data generator
│   └── upload_to_minio.py      # MinIO uploader CLI
├── tests/
│   ├── unit/                   # Pandera, transform, load tests
│   └── integration/            # End-to-end pipeline test
├── docs/                       # Architecture, data dictionary, retention policy
└── .github/workflows/          # CI & CD pipelines
```

## Testing

### Unit Tests
```bash
pip install -r airflow/requirements.txt pytest
pytest tests/unit/ -v
```

### Integration Tests
```bash
# Requires docker compose up -d
pip install pytest requests minio psycopg2-binary
pytest tests/integration/ -v -m integration
```

The integration test reads values from `.env` when present, so its local defaults stay aligned with your stack credentials.

## Security

- **`etl_writer`** role: used by Airflow (read/write on `curated.*` and `audit.*`)
- **`bi_reader`** role: used by Metabase (read-only)
- Secrets in `.env` (gitignored) and GitHub Actions Secrets
- No hardcoded credentials in source code

## Data Quality

Uses **Pandera** for schema validation:
- Type coercion & null checks on all columns
- Regex pattern matching (`order_id`, `customer_id`)
- Range checks (`quantity > 0`, `unit_price > 0`)
- Allowed-value enforcement (`region`)
- Date sanity (not before 2020, not more than 1 day in future)
- Strict mode (rejects unexpected columns)

Invalid rows are separated, quarantined with error reports, and tracked in the audit table.

## CI/CD

- **CI** (`.github/workflows/ci.yml`): Lint → Unit tests → Docker build → Integration test → Data flow validation
- **CD** (`.github/workflows/cd.yml`): Auto-deploy to self-hosted runner on merge to `main`, with health checks

## Shutting Down

```bash
docker compose down        # Stop containers
docker compose down -v     # Stop + remove volumes (⚠️ deletes all data)
```

## Documentation

- [Architecture](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)
- [Retention Policy](docs/retention_policy.md)
- [Data Contract](contracts/sales_contract.yml)
- [Full Blueprint](blueprint.md)
