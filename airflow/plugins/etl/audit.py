"""
audit.py — Write audit rows to audit.etl_audit_runs.

Includes:
- Tenacity retry on DB writes (3 attempts, exponential backoff)
- Connection pooling via pg_client
- Structured logging with run metrics
- AuditError for classified failures
"""
import logging
import uuid
from datetime import datetime, timezone
from psycopg2 import OperationalError
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from utils.pg_client import get_pg_connection, return_pg_connection
from utils.logger import get_logger
from etl.exceptions import AuditError

logger = get_logger(__name__)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def create_audit_run(dag_run_id: str, file_key: str) -> str:
    """
    Insert a RUNNING audit row. Returns the run_id (UUID).
    Retries 3× on OperationalError.
    """
    run_id = str(uuid.uuid4())
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO audit.etl_audit_runs
                (run_id, dag_run_id, file_key, status, started_at)
            VALUES (%s, %s, %s, 'RUNNING', %s)
            """,
            (run_id, dag_run_id, file_key, datetime.now(timezone.utc)),
        )
        conn.commit()
        logger.info(
            "Audit run created: run_id=%s, dag_run=%s, file=%s",
            run_id, dag_run_id, file_key,
            extra={"task_name": "audit", "file_key": file_key},
        )
        return run_id
    except OperationalError:
        conn.rollback()
        raise  # Allow tenacity to retry
    except Exception as e:
        conn.rollback()
        logger.error(
            "Failed to create audit run for %s: %s",
            file_key, str(e), exc_info=True,
            extra={"task_name": "audit", "file_key": file_key},
        )
        raise AuditError(
            f"Audit creation failed for {file_key}: {e}",
            file_key=file_key, step="create_audit_run",
        ) from e
    finally:
        return_pg_connection(conn)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def complete_audit_run(
    run_id: str,
    rows_in: int,
    rows_valid: int,
    rows_loaded: int,
    status: str,
    error_message: str | None = None,
):
    """
    Update an audit row to SUCCESS or FAILED with final metrics.
    Retries 3× on OperationalError.
    """
    conn = get_pg_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE audit.etl_audit_runs SET
                rows_in       = %s,
                rows_valid    = %s,
                rows_loaded   = %s,
                status        = %s,
                error_message = %s,
                finished_at   = %s
            WHERE run_id = %s
            """,
            (
                rows_in,
                rows_valid,
                rows_loaded,
                status,
                error_message,
                datetime.now(timezone.utc),
                run_id,
            ),
        )
        conn.commit()
        logger.info(
            "Audit run %s completed: status=%s, rows_in=%d, valid=%d, loaded=%d, error=%s",
            run_id, status, rows_in, rows_valid, rows_loaded,
            error_message[:100] if error_message else "none",
            extra={"task_name": "audit"},
        )
    except OperationalError:
        conn.rollback()
        raise  # Allow tenacity to retry
    except Exception as e:
        conn.rollback()
        logger.error(
            "Failed to complete audit run %s: %s",
            run_id, str(e), exc_info=True,
            extra={"task_name": "audit"},
        )
        raise AuditError(
            f"Audit completion failed for run {run_id}: {e}",
            step="complete_audit_run",
        ) from e
    finally:
        return_pg_connection(conn)
