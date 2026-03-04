"""
Unit tests for load.py — PostgreSQL UPSERT and summary refresh.

These tests use unittest.mock to avoid needing a live database.
"""
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import date
from etl.load import upsert_orders, refresh_summary


class TestUpsertOrders:
    """Tests for the upsert_orders function."""

    @patch("etl.load.execute_values")
    @patch("etl.load.get_pg_connection")
    def test_inserts_rows(self, mock_conn_factory, mock_execute_values, valid_sales_df):
        """Valid DataFrame should result in execute_values being called."""
        from etl.transform import transform_dataframe
        transformed = transform_dataframe(valid_sales_df.copy())

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_factory.return_value = mock_conn

        rows_loaded = upsert_orders(transformed, "test.csv")
        assert rows_loaded == len(transformed)
        mock_execute_values.assert_called_once()
        mock_conn.commit.assert_called_once()

    @patch("etl.load.get_pg_connection")
    def test_empty_dataframe_loads_zero(self, mock_conn_factory):
        """Empty DataFrame should load 0 rows without error."""
        empty = pd.DataFrame()
        rows_loaded = upsert_orders(empty, "empty.csv")
        assert rows_loaded == 0


class TestRefreshSummary:
    """Tests for the refresh_summary function."""

    @patch("etl.load.get_pg_connection")
    def test_refresh_calls_delete_and_insert(self, mock_conn_factory):
        """Summary refresh should DELETE then INSERT for given dates/regions."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_factory.return_value = mock_conn

        refresh_summary(
            dates=[date(2025, 6, 15)],
            regions=["Europe"],
            file_key="test.csv",
        )
        # Should have called execute twice (DELETE + INSERT)
        assert mock_cursor.execute.call_count == 2
        mock_conn.commit.assert_called_once()

    @patch("etl.load.get_pg_connection")
    def test_empty_dates_skips(self, mock_conn_factory):
        """Empty dates list should skip processing."""
        refresh_summary(dates=[], regions=["Europe"])
        mock_conn_factory.assert_not_called()
