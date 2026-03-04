"""
file_mover.py — Move files to processed/ or quarantine/ in MinIO,
and write error reports to errors/ bucket.

Includes:
- Tenacity retry on MinIO operations (3 attempts, exponential backoff)
- Structured logging at every step
- FileMoveError for classified failures
"""
import io
import json
import logging
from datetime import datetime, timezone
from minio.error import S3Error
from minio.commonconfig import CopySource
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from utils.minio_client import get_minio_client
from utils.logger import get_logger
from etl.exceptions import FileMoveError

logger = get_logger(__name__)

LANDING_BUCKET = "landing"
PROCESSED_BUCKET = "processed"
QUARANTINE_BUCKET = "quarantine"
ERRORS_BUCKET = "errors"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def move_to_processed(file_key: str):
    """
    Copy file from landing/ to processed/sales/<YYYY-MM-DD>/<filename>,
    then delete from landing/.
    Retries 3× on S3/connection errors.
    """
    client = get_minio_client()
    filename = file_key.split("/")[-1]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dest_key = f"sales/{date_str}/{filename}"

    try:
        logger.info(
            "Moving %s → %s/%s",
            file_key, PROCESSED_BUCKET, dest_key,
            extra={"task_name": "move_file", "file_key": file_key},
        )

        # Copy to processed
        client.copy_object(
            PROCESSED_BUCKET,
            dest_key,
            CopySource(LANDING_BUCKET, file_key),
        )
        logger.info(
            "Copied to %s/%s", PROCESSED_BUCKET, dest_key,
            extra={"task_name": "move_file", "file_key": file_key},
        )

        # Delete original
        client.remove_object(LANDING_BUCKET, file_key)
        logger.info(
            "Deleted original from %s/%s",
            LANDING_BUCKET, file_key,
            extra={"task_name": "move_file", "file_key": file_key},
        )

    except S3Error as e:
        logger.error(
            "S3 error moving %s to processed: code=%s, message=%s",
            file_key, e.code, e.message,
            extra={"task_name": "move_file", "file_key": file_key},
        )
        raise FileMoveError(
            f"Failed to move {file_key} to processed: {e}",
            file_key=file_key, step="move_to_processed",
        ) from e
    except Exception as e:
        logger.error(
            "Unexpected error moving %s to processed: %s",
            file_key, str(e), exc_info=True,
            extra={"task_name": "move_file", "file_key": file_key},
        )
        raise FileMoveError(
            f"Move to processed failed for {file_key}: {e}",
            file_key=file_key, step="move_to_processed",
        ) from e


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def move_to_quarantine(file_key: str, run_id: str, errors: list[dict]):
    """
    Copy file to quarantine/ and write an error report to errors/.
    Retries 3× on S3/connection errors.
    """
    client = get_minio_client()
    filename = file_key.split("/")[-1]
    quarantine_key = f"sales/{filename}"

    try:
        logger.warning(
            "Quarantining %s (run_id=%s, %d error(s))",
            file_key, run_id, len(errors),
            extra={"task_name": "move_file", "file_key": file_key},
        )

        # Copy to quarantine
        client.copy_object(
            QUARANTINE_BUCKET,
            quarantine_key,
            CopySource(LANDING_BUCKET, file_key),
        )
        logger.info(
            "Copied to %s/%s", QUARANTINE_BUCKET, quarantine_key,
            extra={"task_name": "move_file", "file_key": file_key},
        )

        # Write error report
        error_report = {
            "run_id": run_id,
            "file_key": file_key,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "FAILED",
            "total_errors": len(errors),
            "errors": errors[:50],  # Cap at 50 errors to avoid huge reports
        }
        if len(errors) > 50:
            error_report["truncated"] = True
            error_report["total_errors_original"] = len(errors)

        report_bytes = json.dumps(error_report, indent=2, default=str).encode("utf-8")
        report_key = f"{run_id}.json"
        client.put_object(
            ERRORS_BUCKET,
            report_key,
            io.BytesIO(report_bytes),
            length=len(report_bytes),
            content_type="application/json",
        )
        logger.info(
            "Error report written: %s/%s (%.1f KB)",
            ERRORS_BUCKET, report_key, len(report_bytes) / 1024,
            extra={"task_name": "move_file", "file_key": file_key},
        )

        # Delete original from landing
        client.remove_object(LANDING_BUCKET, file_key)
        logger.info(
            "Deleted original from %s/%s",
            LANDING_BUCKET, file_key,
            extra={"task_name": "move_file", "file_key": file_key},
        )

    except S3Error as e:
        logger.error(
            "S3 error quarantining %s: code=%s, message=%s",
            file_key, e.code, e.message,
            extra={"task_name": "move_file", "file_key": file_key},
        )
        raise FileMoveError(
            f"Failed to quarantine {file_key}: {e}",
            file_key=file_key, step="move_to_quarantine",
        ) from e
    except Exception as e:
        logger.error(
            "Unexpected error quarantining %s: %s",
            file_key, str(e), exc_info=True,
            extra={"task_name": "move_file", "file_key": file_key},
        )
        raise FileMoveError(
            f"Quarantine failed for {file_key}: {e}",
            file_key=file_key, step="move_to_quarantine",
        ) from e
