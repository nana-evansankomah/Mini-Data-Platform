"""
test_end_to_end.py — Integration test for the full data pipeline.

Prerequisites:
    docker compose up -d  (all services running)

Flow:
    1. Generate a test CSV with known valid + invalid rows
    2. Upload to MinIO landing/sales/
    3. Trigger the Airflow DAG via REST API
    4. Poll until complete
    5. Assert data in PostgreSQL and files moved in MinIO
"""
import os
import time
import csv
import tempfile

import pytest
import requests
import psycopg2
from minio import Minio
from datetime import date, timedelta


def _load_local_env() -> dict[str, str]:
    """Load simple KEY=VALUE pairs from a local .env file when present."""
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    env_path = os.path.join(repo_root, ".env")
    if not os.path.exists(env_path):
        return {}

    env = {}
    with open(env_path, encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env


LOCAL_ENV = _load_local_env()


# ── Configuration ─────────────────────────────────────────────
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv(
    "AIRFLOW_PASS",
    LOCAL_ENV.get("AIRFLOW_ADMIN_PASSWORD", "changeme"),
)

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT_EXT",
    f"localhost:{LOCAL_ENV.get('MINIO_API_PORT', '9000')}",
)
MINIO_ACCESS = os.getenv(
    "MINIO_ACCESS_KEY",
    LOCAL_ENV.get("MINIO_ROOT_USER", "minioadmin"),
)
MINIO_SECRET = os.getenv(
    "MINIO_SECRET_KEY",
    LOCAL_ENV.get("MINIO_ROOT_PASSWORD", "changeme123"),
)

_pg_dsn_default = (
    "postgresql://etl_writer:"
    f"{LOCAL_ENV.get('ETL_WRITER_PASSWORD', 'changeme')}"
    "@localhost:"
    f"{LOCAL_ENV.get('POSTGRES_HOST_PORT', '5432')}/"
    f"{LOCAL_ENV.get('POSTGRES_DB', 'dataplatform')}"
)
PG_DSN = os.getenv("PG_DSN", _pg_dsn_default)

VALID_ROWS = 5
INVALID_ROWS = 2
DAG_ID = "sales_etl"
POLL_TIMEOUT = 600  # seconds
HEALTH_TIMEOUT = 180  # seconds


# ── Helpers ───────────────────────────────────────────────────
def generate_test_csv(path: str, valid: int, invalid: int):
    """Generate a test CSV with known data."""
    fieldnames = [
        "order_id", "order_date", "customer_id",
        "region", "product", "quantity", "unit_price",
    ]
    rows = []
    for i in range(1, valid + 1):
        rows.append({
            "order_id": f"ORD-{90000 + i:06d}",
            "order_date": (date.today() - timedelta(days=i)).isoformat(),
            "customer_id": f"CUST-{1000 + i}",
            "region": "Europe",
            "product": "Integration Test Widget",
            "quantity": i * 10,
            "unit_price": round(9.99 + i, 2),
        })
    for i in range(1, invalid + 1):
        rows.append({
            "order_id": f"BADINVALID{i}",
            "order_date": "2030-12-31",
            "customer_id": f"CUST-{2000 + i}",
            "region": "Atlantis",
            "product": "X",
            "quantity": -1,
            "unit_price": 0.00,
        })
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def trigger_dag(dag_id: str) -> str:
    """Trigger an Airflow DAG and return the dag_run_id."""
    resp = requests.post(
        f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
        json={"conf": {}},
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["dag_run_id"]


def wait_for_airflow(timeout: int):
    """Wait until Airflow API health endpoint responds successfully."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(
                f"{AIRFLOW_URL}/health",
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
                timeout=10,
            )
            if resp.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise TimeoutError(f"Airflow did not become healthy within {timeout}s")


def wait_for_dag(dag_id: str, run_id: str, timeout: int):
    """Poll until DAG run completes or timeout."""
    start = time.time()
    while time.time() - start < timeout:
        resp = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}",
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=30,
        )
        resp.raise_for_status()
        state = resp.json()["state"]
        if state in ("success", "failed"):
            return state
        time.sleep(10)
    raise TimeoutError(f"DAG {dag_id} run {run_id} did not complete within {timeout}s")


# ── Test ──────────────────────────────────────────────────────
@pytest.mark.integration
class TestEndToEnd:
    """Full pipeline integration test."""

    def test_pipeline_processes_csv(self):
        """Upload CSV → trigger DAG → assert Postgres data + file moved."""
        # 1. Generate test CSV
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            csv_path = f.name

        try:
            generate_test_csv(csv_path, VALID_ROWS, INVALID_ROWS)

            # 2. Upload to MinIO
            minio_client = Minio(
                MINIO_ENDPOINT, access_key=MINIO_ACCESS,
                secret_key=MINIO_SECRET, secure=False,
            )
            filename = os.path.basename(csv_path)
            object_name = f"sales/{filename}"
            minio_client.fput_object("landing", object_name, csv_path)

            # 3. Ensure Airflow is ready before triggering
            wait_for_airflow(HEALTH_TIMEOUT)
            run_id = trigger_dag(DAG_ID)

            # 4. Wait for completion
            state = wait_for_dag(DAG_ID, run_id, POLL_TIMEOUT)
            assert state == "success", f"DAG run ended with state: {state}"

            # 5. Assert PostgreSQL data
            conn = psycopg2.connect(PG_DSN)
            cur = conn.cursor()

            # Check fact table
            cur.execute(
                "SELECT COUNT(*) FROM curated.fact_orders WHERE source_file = %s",
                (object_name,),
            )
            fact_count = cur.fetchone()[0]
            assert fact_count == VALID_ROWS, f"Expected {VALID_ROWS} rows, got {fact_count}"

            # Check audit table
            cur.execute(
                "SELECT status, rows_in, rows_valid, rows_loaded "
                "FROM audit.etl_audit_runs "
                "WHERE dag_run_id = %s AND file_key = %s "
                "ORDER BY started_at DESC LIMIT 1",
                (run_id, object_name),
            )
            audit_row = cur.fetchone()
            assert audit_row is not None, "Expected an audit row for the uploaded file"
            assert audit_row[0] == "SUCCESS"
            assert audit_row[1] == VALID_ROWS + INVALID_ROWS
            assert audit_row[2] == VALID_ROWS
            assert audit_row[3] == VALID_ROWS

            conn.close()

            # 6. Assert file moved from landing
            remaining = list(
                minio_client.list_objects("landing", prefix="sales/", recursive=True)
            )
            landing_files = [o.object_name for o in remaining if filename in o.object_name]
            assert len(landing_files) == 0, "File should have been moved from landing/"
        finally:
            if os.path.exists(csv_path):
                os.unlink(csv_path)
