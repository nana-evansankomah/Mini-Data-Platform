"""
load.py — UPSERT into PostgreSQL fact table and refresh summary.

Includes:
- Tenacity retry on DB operations (3 attempts, exponential backoff)
- Connection pooling via pg_client
- Batch size control for large DataFrames
- Structured logging with row counts and timing
- LoadError for classified failures
"""
import logging
import time
import pandas as pd
from psycopg2.extras import execute_values
from psycopg2 import OperationalError, IntegrityError
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from utils.pg_client import get_pg_connection, return_pg_connection
from utils.logger import get_logger
from etl.exceptions import LoadError

logger = get_logger(__name__)

BATCH_SIZE = 1000  # rows per batch for large DataFrames


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def upsert_orders(df: pd.DataFrame, source_file: str) -> int:
    """
    UPSERT rows into curated.fact_orders.
    Retries 3× on OperationalError (connection/timeout).
    Processes in batches of BATCH_SIZE for memory efficiency.
    Returns the number of rows loaded.
    """
    if df.empty:
        logger.info(
            "No rows to load — skipping upsert",
            extra={"task_name": "load", "file_key": source_file},
        )
        return 0

    start_time = time.time()
    conn = get_pg_connection()
    total_loaded = 0

    try:
        cur = conn.cursor()

        # Prepare all rows
        rows = []
        for _, r in df.iterrows():
            rows.append((
                r["order_id"],
                r["order_date"].date() if hasattr(r["order_date"], "date") else r["order_date"],
                r["customer_id"],
                r["region"],
                r["product"],
                int(r["quantity"]),
                float(r["unit_price"]),
                float(r["total_amount"]),
                source_file,
            ))

        sql = """
            INSERT INTO curated.fact_orders
                (order_id, order_date, customer_id, region, product,
                 quantity, unit_price, total_amount, source_file)
            VALUES %s
            ON CONFLICT (order_id) DO UPDATE SET
                order_date   = EXCLUDED.order_date,
                customer_id  = EXCLUDED.customer_id,
                region       = EXCLUDED.region,
                product      = EXCLUDED.product,
                quantity     = EXCLUDED.quantity,
                unit_price   = EXCLUDED.unit_price,
                total_amount = EXCLUDED.total_amount,
                source_file  = EXCLUDED.source_file,
                loaded_at    = NOW()
        """

        # Process in batches
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            execute_values(cur, sql, batch)
            total_loaded += len(batch)
            logger.info(
                "Batch %d: upserted %d rows (total: %d/%d)",
                (i // BATCH_SIZE) + 1, len(batch), total_loaded, len(rows),
                extra={"task_name": "load", "file_key": source_file},
            )

        conn.commit()
        elapsed = time.time() - start_time

        logger.info(
            "Upserted %d rows into curated.fact_orders in %.2fs (%.0f rows/sec)",
            total_loaded, elapsed, total_loaded / elapsed if elapsed > 0 else 0,
            extra={"task_name": "load", "file_key": source_file},
        )
        return total_loaded

    except IntegrityError as e:
        conn.rollback()
        logger.error(
            "Integrity constraint violation loading %s: %s",
            source_file, str(e),
            extra={"task_name": "load", "file_key": source_file},
        )
        raise LoadError(
            f"Integrity error loading {source_file}: {e}",
            file_key=source_file, step="upsert_orders",
        ) from e
    except OperationalError as e:
        conn.rollback()
        logger.error(
            "PG operational error loading %s: %s",
            source_file, str(e),
            extra={"task_name": "load", "file_key": source_file},
        )
        raise  # Allow tenacity to retry
    except Exception as e:
        conn.rollback()
        logger.error(
            "Unexpected error loading %s: %s",
            source_file, str(e), exc_info=True,
            extra={"task_name": "load", "file_key": source_file},
        )
        raise LoadError(
            f"Load failed for {source_file}: {e}",
            file_key=source_file, step="upsert_orders",
        ) from e
    finally:
        return_pg_connection(conn)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def refresh_summary(dates: list, regions: list, file_key: str = ""):
    """
    Refresh curated.summary_daily_sales for the given dates and regions.
    Uses DELETE + INSERT to ensure idempotent aggregations.
    Retries 3× on OperationalError.
    """
    if not dates or not regions:
        logger.info(
            "No dates/regions to refresh — skipping summary",
            extra={"task_name": "refresh_summary", "file_key": file_key},
        )
        return

    start_time = time.time()
    conn = get_pg_connection()

    try:
        cur = conn.cursor()
        date_values = [pd.to_datetime(d).date() for d in dates]

        logger.info(
            "Refreshing summary: %d date(s) × %d region(s)",
            len(date_values), len(regions),
            extra={"task_name": "refresh_summary", "file_key": file_key},
        )

        # Delete existing summaries for affected combos
        cur.execute(
            """
            DELETE FROM curated.summary_daily_sales
            WHERE summary_date = ANY(%s::date[]) AND region = ANY(%s)
            """,
            (date_values, regions),
        )
        deleted = cur.rowcount
        logger.info(
            "Deleted %d stale summary rows",
            deleted,
            extra={"task_name": "refresh_summary", "file_key": file_key},
        )

        # Re-aggregate from fact table
        cur.execute(
            """
            INSERT INTO curated.summary_daily_sales
                (summary_date, region, total_orders, total_quantity,
                 total_revenue, avg_order_value, updated_at)
            SELECT
                order_date,
                region,
                COUNT(*)::int,
                SUM(quantity)::bigint,
                SUM(total_amount)::numeric(14,2),
                ROUND(AVG(total_amount)::numeric, 2),
                NOW()
            FROM curated.fact_orders
            WHERE order_date = ANY(%s::date[]) AND region = ANY(%s)
            GROUP BY order_date, region
            """,
            (date_values, regions),
        )
        inserted = cur.rowcount

        conn.commit()
        elapsed = time.time() - start_time
        logger.info(
            "Summary refreshed: %d rows inserted (replaced %d) in %.2fs",
            inserted, deleted, elapsed,
            extra={"task_name": "refresh_summary", "file_key": file_key},
        )

    except OperationalError:
        conn.rollback()
        raise  # Allow tenacity to retry
    except Exception as e:
        conn.rollback()
        logger.error(
            "Summary refresh failed: %s", str(e), exc_info=True,
            extra={"task_name": "refresh_summary", "file_key": file_key},
        )
        raise LoadError(
            f"Summary refresh failed: {e}",
            file_key=file_key, step="refresh_summary",
        ) from e
    finally:
        return_pg_connection(conn)
