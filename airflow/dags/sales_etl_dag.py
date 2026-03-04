"""
sales_etl_dag.py — Main Airflow DAG for the Sales ETL pipeline.

Features:
- ThreadPoolExecutor for concurrent file processing
- Airflow failure/success/SLA callbacks
- Per-file error isolation (one bad file doesn't block others)
- Structured logging with context enrichment
- Rich DAG-level metrics logging

Flow: discover → process_files (concurrent per file:
    validate → transform → load → refresh_summary → audit → move_file)
"""
import sys
import os
import time
import csv
import random
import tempfile
import traceback
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# Ensure plugins directory is on the path
sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "plugins"))

from etl.extract import discover_files, download_csv
from etl.validate import validate_dataframe
from etl.transform import transform_dataframe
from etl.load import upsert_orders, refresh_summary
from etl.audit import create_audit_run, complete_audit_run
from etl.file_mover import move_to_processed, move_to_quarantine
from etl.exceptions import ETLError
from utils.logger import get_logger, log_context
from utils.pg_client import close_pool
from utils.minio_client import get_minio_client, minio_operation

logger = get_logger("sales_etl_dag")

# ── Concurrency config ───────────────────────────────────────
MAX_CONCURRENT_FILES = int(os.getenv("ETL_MAX_WORKERS", "4"))
AUTO_GENERATE_ENABLED = os.getenv("ETL_AUTO_GENERATE", "true").lower() in {"1", "true", "yes", "on"}
AUTO_GENERATE_ROWS = int(os.getenv("ETL_AUTO_GENERATE_ROWS", "50"))
AUTO_GENERATE_INVALID_ROWS = int(os.getenv("ETL_AUTO_GENERATE_INVALID_ROWS", "0"))
LANDING_BUCKET = "landing"
LANDING_PREFIX = "sales/"

REGIONS = [
    "North America",
    "Europe",
    "Asia Pacific",
    "Latin America",
    "Middle East & Africa",
]

PRODUCTS = [
    "Widget Pro",
    "Gadget X",
    "ThingaMajig 3000",
    "Super Connector",
    "Data Cable Premium",
    "Cloud Storage Box",
    "Smart Sensor Kit",
    "Power Adapter Plus",
    "Wireless Hub",
    "Mega Battery Pack",
]


# ── Airflow callbacks ────────────────────────────────────────
def _on_failure(context):
    """Called when a task fails — logs full context for debugging."""
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    logger.error(
        "TASK FAILED: dag=%s, task=%s, run=%s, exception=%s, traceback=%s",
        context.get("dag").dag_id,
        task_instance.task_id if task_instance else "unknown",
        context.get("run_id", "unknown"),
        str(exception),
        traceback.format_exc() if exception else "none",
        extra={"task_name": "on_failure_callback"},
    )


def _on_success(context):
    """Called when DAG run succeeds — logs summary."""
    logger.info(
        "DAG RUN SUCCESS: dag=%s, run=%s, execution_date=%s",
        context.get("dag").dag_id,
        context.get("run_id", "unknown"),
        str(context.get("execution_date", "")),
        extra={"task_name": "on_success_callback"},
    )


def _sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when SLA is missed."""
    logger.warning(
        "SLA MISS: dag=%s, tasks=%s",
        dag.dag_id,
        [str(t) for t in task_list],
        extra={"task_name": "sla_miss_callback"},
    )


# ── Default args ──────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": _on_failure,
}


# ── Per-file processing ──────────────────────────────────────
def _process_single_file(file_key: str, dag_run_id: str) -> dict:
    """
    Process a single CSV file through the full pipeline.
    Returns a result dict with metrics and status.

    This function is designed to be called from a ThreadPoolExecutor.
    It is fully self-contained with its own error handling so one
    file failure doesn't affect others.
    """
    result = {
        "file_key": file_key,
        "status": "FAILED",
        "rows_in": 0,
        "rows_valid": 0,
        "rows_loaded": 0,
        "error": None,
        "run_id": None,
        "elapsed": 0.0,
    }
    start_time = time.time()

    with log_context(file_key=file_key, dag_run_id=dag_run_id):
        try:
            # ── Audit: start ──
            run_id = create_audit_run(dag_run_id, file_key)
            result["run_id"] = run_id
            logger.info("[START] Pipeline started for %s (run_id=%s)", file_key, run_id)

            # ── Extract ──
            logger.info("Step 1/6: Extracting %s", file_key)
            df = download_csv(file_key)
            result["rows_in"] = len(df)
            logger.info("  → Extracted %d rows", len(df))

            # ── Validate ──
            logger.info("Step 2/6: Validating %s", file_key)
            valid_df, errors = validate_dataframe(df, file_key)
            result["rows_valid"] = len(valid_df)
            logger.info(
                "  → %d valid, %d invalid",
                len(valid_df), len(errors),
            )

            if valid_df.empty:
                error_msg = f"All {result['rows_in']} rows failed validation"
                logger.warning("  → %s — quarantining", error_msg)
                complete_audit_run(
                    run_id, result["rows_in"], 0, 0,
                    "FAILED", error_msg,
                )
                move_to_quarantine(file_key, run_id, errors)
                result["error"] = error_msg
                result["elapsed"] = time.time() - start_time
                return result

            # ── Transform ──
            logger.info("Step 3/6: Transforming %s", file_key)
            transformed_df = transform_dataframe(valid_df, file_key)
            logger.info("  → %d rows after transform", len(transformed_df))

            # ── Load ──
            logger.info("Step 4/6: Loading %s into PostgreSQL", file_key)
            rows_loaded = upsert_orders(transformed_df, file_key)
            result["rows_loaded"] = rows_loaded
            logger.info("  → %d rows upserted", rows_loaded)

            # ── Refresh summary ──
            logger.info("Step 5/6: Refreshing summary tables")
            affected_dates = transformed_df["order_date"].dt.date.unique().tolist()
            affected_regions = transformed_df["region"].unique().tolist()
            refresh_summary(affected_dates, affected_regions, file_key)
            logger.info(
                "  → Summary refreshed for %d dates × %d regions",
                len(affected_dates), len(affected_regions),
            )

            # ── Audit: success ──
            logger.info("Step 6/6: Finalizing audit for %s", file_key)
            error_msg = None
            if errors:
                error_msg = f"{len(errors)} row(s) failed validation and were skipped"
            complete_audit_run(
                run_id, result["rows_in"], result["rows_valid"],
                rows_loaded, "SUCCESS", error_msg,
            )

            # ── Move to processed ──
            move_to_processed(file_key)

            result["status"] = "SUCCESS"
            result["elapsed"] = time.time() - start_time
            logger.info(
                "[OK] Pipeline complete for %s: %d->%d->%d rows in %.2fs",
                file_key, result["rows_in"], result["rows_valid"],
                rows_loaded, result["elapsed"],
            )

        except ETLError as exc:
            result["error"] = f"[{exc.step}] {str(exc)}"
            result["elapsed"] = time.time() - start_time
            logger.error(
                "[FAIL] ETL error for %s at step '%s': %s",
                file_key, exc.step, str(exc), exc_info=True,
            )
            _handle_file_failure(result, file_key, errors=[] if 'errors' not in dir() else errors)

        except Exception as exc:
            result["error"] = str(exc)
            result["elapsed"] = time.time() - start_time
            logger.error(
                "[FAIL] Unexpected error for %s: %s",
                file_key, str(exc), exc_info=True,
            )
            _handle_file_failure(result, file_key, errors=[{"error": str(exc)}])

    return result


def _handle_file_failure(result: dict, file_key: str, errors: list):
    """Handle a failed file: update audit, attempt quarantine."""
    run_id = result.get("run_id")
    if run_id:
        try:
            complete_audit_run(
                run_id, result["rows_in"], result["rows_valid"],
                result["rows_loaded"], "FAILED", result["error"],
            )
        except Exception as audit_err:
            logger.error(
                "Failed to write audit for %s: %s",
                file_key, str(audit_err), exc_info=True,
            )
        try:
            move_to_quarantine(file_key, run_id, errors)
        except Exception as move_err:
            logger.error(
                "Failed to quarantine %s: %s — file may remain in landing/",
                file_key, str(move_err), exc_info=True,
            )


# ── Task callables ────────────────────────────────────────────
def _discover(**context):
    """Discover CSV files and push keys to XCom."""
    logger.info("Starting file discovery...")
    keys = discover_files()
    context["ti"].xcom_push(key="file_keys", value=keys)
    if keys:
        logger.info(
            "Found %d file(s) to process: %s",
            len(keys), ", ".join(keys),
        )
    else:
        logger.info("No files found in landing zone — nothing to do")
    return len(keys) > 0


def _process_files(**context):
    """
    Process all discovered files using ThreadPoolExecutor for concurrency.
    Each file runs in its own thread with independent error handling.
    """
    ti = context["ti"]
    dag_run_id = context["run_id"]
    file_keys = ti.xcom_pull(task_ids="discover_files", key="file_keys") or []

    if not file_keys:
        logger.info("No files to process")
        return

    total_start = time.time()
    logger.info(
        "Processing %d file(s) with %d concurrent workers",
        len(file_keys), min(MAX_CONCURRENT_FILES, len(file_keys)),
    )

    results = []

    # ── Concurrent processing with ThreadPoolExecutor ──
    with ThreadPoolExecutor(
        max_workers=min(MAX_CONCURRENT_FILES, len(file_keys)),
        thread_name_prefix="etl_worker",
    ) as executor:
        # Submit all files
        future_to_key = {
            executor.submit(_process_single_file, key, dag_run_id): key
            for key in file_keys
        }

        # Collect results as they complete
        for future in as_completed(future_to_key):
            file_key = future_to_key[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                logger.error(
                    "Thread crashed for %s: %s", file_key, str(exc),
                    exc_info=True,
                )
                results.append({
                    "file_key": file_key,
                    "status": "FAILED",
                    "error": f"Thread exception: {exc}",
                    "rows_in": 0,
                    "rows_valid": 0,
                    "rows_loaded": 0,
                    "elapsed": 0.0,
                })

    # ── Log DAG-level summary ──
    total_elapsed = time.time() - total_start
    succeeded = [r for r in results if r["status"] == "SUCCESS"]
    failed = [r for r in results if r["status"] == "FAILED"]

    total_rows_in = sum(r["rows_in"] for r in results)
    total_rows_loaded = sum(r["rows_loaded"] for r in results)

    logger.info("=" * 60)
    logger.info("DAG RUN SUMMARY")
    logger.info("=" * 60)
    logger.info("  Files processed:  %d", len(results))
    logger.info("  Succeeded:        %d", len(succeeded))
    logger.info("  Failed:           %d", len(failed))
    logger.info("  Total rows in:    %d", total_rows_in)
    logger.info("  Total rows loaded:%d", total_rows_loaded)
    logger.info("  Total time:       %.2fs", total_elapsed)
    logger.info(
        "  Throughput:       %.0f rows/sec",
        total_rows_loaded / total_elapsed if total_elapsed > 0 else 0,
    )

    if failed:
        logger.warning("Failed files:")
        for r in failed:
            logger.warning(
                "  [FAIL] %s -- %s", r["file_key"], r.get("error", "unknown"),
            )

    logger.info("=" * 60)

    # Push summary to XCom for downstream tasks or alerting
    ti.xcom_push(key="etl_summary", value={
        "total_files": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "total_rows_in": total_rows_in,
        "total_rows_loaded": total_rows_loaded,
        "elapsed_seconds": round(total_elapsed, 2),
        "failed_files": [r["file_key"] for r in failed],
    })

    # If ALL files failed, raise to trigger Airflow retry
    if failed and not succeeded:
        raise RuntimeError(
            f"All {len(failed)} file(s) failed processing. "
            f"Errors: {[r.get('error', 'unknown') for r in failed]}"
        )


def _cleanup(**context):
    """Cleanup: close connection pool after processing."""
    logger.info("Running cleanup: closing PG connection pool")
    try:
        close_pool()
        logger.info("Cleanup complete")
    except Exception as e:
        logger.warning("Cleanup error (non-fatal): %s", str(e))


def _generate_valid_row(order_num: int) -> dict:
    """Generate one valid sales row."""
    order_date = date.today() - timedelta(days=random.randint(0, 365))
    return {
        "order_id": f"ORD-{order_num}",
        "order_date": order_date.isoformat(),
        "customer_id": f"CUST-{random.randint(100000, 999999)}",
        "region": random.choice(REGIONS),
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 500),
        "unit_price": round(random.uniform(5.00, 999.99), 2),
    }


def _generate_invalid_row(order_num: int) -> dict:
    """Generate one invalid row to exercise quarantine behavior."""
    error_type = random.choice(["bad_id", "future_date", "bad_region", "neg_qty", "zero_price"])
    row = _generate_valid_row(order_num)
    if error_type == "bad_id":
        row["order_id"] = str(random.randint(10000, 99999))
    elif error_type == "future_date":
        row["order_date"] = "2030-12-31"
    elif error_type == "bad_region":
        row["region"] = "Antarctica"
    elif error_type == "neg_qty":
        row["quantity"] = -random.randint(1, 100)
    elif error_type == "zero_price":
        row["unit_price"] = 0.00
    return row


def _generate_sample_data(**context):
    """
    Generate a CSV file for this DAG run.

    Returns the local file path via XCom.
    """
    if not AUTO_GENERATE_ENABLED:
        logger.info("Auto-generation disabled (ETL_AUTO_GENERATE=false); skipping sample file generation")
        return None

    rows = max(AUTO_GENERATE_ROWS, 0)
    invalid_rows = max(AUTO_GENERATE_INVALID_ROWS, 0)
    if rows == 0 and invalid_rows == 0:
        logger.info("Auto-generation configured with 0 rows; skipping sample file generation")
        return None
    fieldnames = ["order_id", "order_date", "customer_id", "region", "product", "quantity", "unit_price"]

    run_id = context.get("run_id", "manual__run").replace(":", "_")
    run_id = run_id.replace("+", "_").replace("-", "_")
    filename = f"sample_sales_{run_id}.csv"
    file_path = os.path.join(tempfile.gettempdir(), filename)

    # Keep identifiers in 7-10 digits to satisfy the data contract.
    start_id = random.randint(5_000_000, 9_000_000)
    records = [_generate_valid_row(start_id + i) for i in range(rows)]
    records.extend(_generate_invalid_row(start_id + rows + i) for i in range(invalid_rows))
    random.shuffle(records)

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    logger.info(
        "Generated sample file: %s (%d valid + %d invalid rows)",
        file_path, rows, invalid_rows,
    )
    return file_path


def _upload_sample_data(**context):
    """
    Upload generated sample CSV to landing/sales/ in MinIO.

    Returns the uploaded object key via XCom.
    """
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="generate_sample_data")
    if not file_path:
        logger.info("No generated sample file to upload; skipping upload task")
        return None

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Generated sample file not found: {file_path}")

    object_name = f"{LANDING_PREFIX}{os.path.basename(file_path)}"
    client = get_minio_client()

    if not minio_operation(client.bucket_exists, LANDING_BUCKET):
        minio_operation(client.make_bucket, LANDING_BUCKET)

    minio_operation(client.fput_object, LANDING_BUCKET, object_name, file_path)
    logger.info("Uploaded sample file to s3://%s/%s", LANDING_BUCKET, object_name)

    try:
        os.remove(file_path)
    except OSError as exc:
        logger.warning("Could not remove temp sample file %s: %s", file_path, str(exc))

    return object_name


# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id="sales_etl",
    default_args=default_args,
    description="Sales CSV ETL: MinIO → validate → transform → PostgreSQL (concurrent)",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "sales"],
    on_success_callback=_on_success,
    sla_miss_callback=_sla_miss,
) as dag:
    generate = PythonOperator(
        task_id="generate_sample_data",
        python_callable=_generate_sample_data,
        provide_context=True,
    )

    upload = PythonOperator(
        task_id="upload_sample_data",
        python_callable=_upload_sample_data,
        provide_context=True,
    )

    discover = ShortCircuitOperator(
        task_id="discover_files",
        python_callable=_discover,
        provide_context=True,
    )

    process = PythonOperator(
        task_id="process_files",
        python_callable=_process_files,
        provide_context=True,
        retries=0,
    )

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=_cleanup,
        provide_context=True,
        trigger_rule="all_done",  # Always runs, even if process fails
    )

    generate >> upload >> discover >> process >> cleanup
