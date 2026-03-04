"""
PostgreSQL connection helper — connection pooling, retry, and logging.
"""
import os
import threading
import logging
from psycopg2 import pool, OperationalError, InterfaceError
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
from utils.logger import get_logger

logger = get_logger(__name__)

# Thread-safe connection pool (lazy init)
_pool: pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()

POOL_MIN = 2
POOL_MAX = 10


def _init_pool():
    """Initialize the threaded connection pool (called once)."""
    global _pool
    host = os.getenv("PG_HOST", "postgres")
    port = int(os.getenv("PG_PORT", "5432"))
    dbname = os.getenv("PG_DATABASE", "dataplatform")
    user = os.getenv("PG_USER_ETL", "etl_writer")
    password = os.getenv("PG_PASSWORD_ETL", "changeme")

    logger.info(
        "Initializing PG connection pool: %s@%s:%d/%s (min=%d, max=%d)",
        user, host, port, dbname, POOL_MIN, POOL_MAX,
    )
    _pool = pool.ThreadedConnectionPool(
        POOL_MIN,
        POOL_MAX,
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def get_pg_connection():
    """
    Get a connection from the pool with retry.
    Retries 3× with exponential backoff on OperationalError (e.g. PG down).
    Caller must call return_pg_connection() when done.
    """
    global _pool
    with _pool_lock:
        if _pool is None:
            _init_pool()
    try:
        conn = _pool.getconn()
        # Test the connection is alive
        conn.isolation_level
        return conn
    except (OperationalError, InterfaceError) as e:
        logger.error("PG connection failed: %s", str(e))
        # Reset the pool on connection failure
        with _pool_lock:
            try:
                _pool.closeall()
            except Exception:
                pass
            _pool = None
            _init_pool()
        raise


def return_pg_connection(conn):
    """Return a connection to the pool."""
    global _pool
    if _pool and conn:
        try:
            _pool.putconn(conn)
        except Exception as e:
            logger.warning("Failed to return connection to pool: %s", str(e))
            try:
                conn.close()
            except Exception:
                pass


def close_pool():
    """Shutdown the connection pool (call on DAG teardown)."""
    global _pool
    with _pool_lock:
        if _pool:
            _pool.closeall()
            _pool = None
            logger.info("PG connection pool closed")
