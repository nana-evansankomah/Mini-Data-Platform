"""
extract.py — Discover and download CSV files from MinIO landing zone.

Includes:
- Tenacity retry on MinIO operations (3 attempts, exponential backoff)
- Structured logging at every stage
- ExtractionError for classified failures
"""
import io
import logging
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
from minio.error import S3Error

from utils.minio_client import get_minio_client
from utils.logger import get_logger
from etl.exceptions import ExtractionError

logger = get_logger(__name__)

LANDING_BUCKET = "landing"
LANDING_PREFIX = "sales/"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def discover_files() -> list[str]:
    """
    List all CSV objects in the MinIO landing/sales/ prefix.
    Retries 3× with exponential backoff on S3/connection errors.
    """
    try:
        client = get_minio_client()
        logger.info("Scanning %s/%s for CSV files...", LANDING_BUCKET, LANDING_PREFIX)
        objects = client.list_objects(LANDING_BUCKET, prefix=LANDING_PREFIX, recursive=True)
        keys = [
            obj.object_name
            for obj in objects
            if obj.object_name.endswith(".csv")
        ]
        logger.info(
            "Discovered %d CSV file(s) in %s/%s",
            len(keys), LANDING_BUCKET, LANDING_PREFIX,
            extra={"task_name": "discover_files"},
        )
        return keys
    except S3Error as e:
        logger.error(
            "MinIO S3 error during discovery: code=%s, message=%s",
            e.code, e.message,
            extra={"task_name": "discover_files"},
        )
        raise ExtractionError(
            f"Failed to list objects in {LANDING_BUCKET}/{LANDING_PREFIX}: {e}",
            step="discover_files",
        ) from e
    except Exception as e:
        logger.error(
            "Unexpected error during file discovery: %s",
            str(e), exc_info=True,
            extra={"task_name": "discover_files"},
        )
        raise ExtractionError(
            f"Discovery failed: {e}", step="discover_files"
        ) from e


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def download_csv(file_key: str) -> pd.DataFrame:
    """
    Download a CSV from MinIO and return as a DataFrame.
    Retries 3× with exponential backoff on S3/connection errors.
    """
    logger.info(
        "Downloading %s from %s...", file_key, LANDING_BUCKET,
        extra={"task_name": "download_csv", "file_key": file_key},
    )
    response = None
    try:
        client = get_minio_client()
        response = client.get_object(LANDING_BUCKET, file_key)
        data = response.read()
        df = pd.read_csv(io.BytesIO(data))
        logger.info(
            "Downloaded %s — %d rows, %d columns, %.1f KB",
            file_key, len(df), len(df.columns), len(data) / 1024,
            extra={"task_name": "download_csv", "file_key": file_key},
        )
        return df
    except S3Error as e:
        logger.error(
            "MinIO S3 error downloading %s: code=%s, message=%s",
            file_key, e.code, e.message,
            extra={"task_name": "download_csv", "file_key": file_key},
        )
        raise ExtractionError(
            f"Failed to download {file_key}: {e}",
            file_key=file_key, step="download_csv",
        ) from e
    except pd.errors.ParserError as e:
        logger.error(
            "CSV parse error for %s: %s", file_key, str(e),
            extra={"task_name": "download_csv", "file_key": file_key},
        )
        raise ExtractionError(
            f"CSV parsing failed for {file_key}: {e}",
            file_key=file_key, step="download_csv",
        ) from e
    except Exception as e:
        logger.error(
            "Unexpected error downloading %s: %s",
            file_key, str(e), exc_info=True,
            extra={"task_name": "download_csv", "file_key": file_key},
        )
        raise ExtractionError(
            f"Download failed for {file_key}: {e}",
            file_key=file_key, step="download_csv",
        ) from e
    finally:
        if response:
            try:
                response.close()
                response.release_conn()
            except Exception:
                pass
