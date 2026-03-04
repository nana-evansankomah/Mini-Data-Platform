"""
MinIO client helper — wraps connection setup with retry logic and logging.
"""
import logging
import os
from minio import Minio
from minio.error import S3Error
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
from utils.logger import get_logger

logger = get_logger(__name__)

# Module-level client cache (MinIO client is thread-safe)
_client: Minio | None = None


def get_minio_client() -> Minio:
    """Return a configured MinIO client (cached singleton)."""
    global _client
    if _client is None:
        endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        logger.info("Initializing MinIO client → %s", endpoint)
        _client = Minio(
            endpoint=endpoint,
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "changeme123"),
            secure=False,
        )
    return _client


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def minio_operation(func, *args, **kwargs):
    """
    Execute a MinIO operation with retry.
    Wraps any callable with 3 attempts + exponential backoff.

    Usage:
        result = minio_operation(client.list_objects, "bucket", prefix="sales/")
    """
    try:
        return func(*args, **kwargs)
    except S3Error as e:
        logger.error(
            "MinIO S3Error: code=%s, message=%s, resource=%s",
            e.code, e.message, e.resource,
        )
        raise
    except Exception as e:
        logger.error("MinIO operation failed: %s", str(e), exc_info=True)
        raise
