"""
Unit tests for validate.py — Pandera schema validation.
"""
from etl.validate import validate_dataframe


class TestValidateDataframe:
    """Tests for the validate_dataframe function."""

    def test_valid_dataframe_passes(self, valid_sales_df):
        """All valid rows should pass without errors."""
        valid_df, errors = validate_dataframe(valid_sales_df)
        assert len(valid_df) == 3
        assert len(errors) == 0

    def test_missing_order_id_fails(self, valid_sales_df):
        """Null order_id should trigger a validation error."""
        valid_sales_df.loc[0, "order_id"] = None
        valid_df, errors = validate_dataframe(valid_sales_df)
        assert len(errors) > 0

    def test_invalid_order_id_pattern_fails(self, valid_sales_df):
        """order_id not matching ORD-NNNNNN should fail."""
        valid_sales_df.loc[0, "order_id"] = "12345"
        valid_df, errors = validate_dataframe(valid_sales_df)
        assert len(errors) > 0

    def test_negative_quantity_fails(self, valid_sales_df):
        """Negative quantity should fail the > 0 check."""
        valid_sales_df.loc[0, "quantity"] = -5
        valid_df, errors = validate_dataframe(valid_sales_df)
        assert len(errors) > 0

    def test_future_date_fails(self, valid_sales_df):
        """Date far in the future should fail."""
        valid_sales_df.loc[0, "order_date"] = "2030-01-01"
        valid_df, errors = validate_dataframe(valid_sales_df)
        assert len(errors) > 0

    def test_invalid_region_fails(self, valid_sales_df):
        """Region not in allowed list should fail."""
        valid_sales_df.loc[0, "region"] = "Antarctica"
        valid_df, errors = validate_dataframe(valid_sales_df)
        assert len(errors) > 0

    def test_extra_column_fails(self, valid_sales_df):
        """Extra column should fail strict mode."""
        valid_sales_df["extra_col"] = "surprise"
        valid_df, errors = validate_dataframe(valid_sales_df)
        assert len(errors) > 0

    def test_mixed_valid_and_invalid(self, invalid_sales_df):
        """Mixed DataFrame should return only valid rows."""
        valid_df, errors = validate_dataframe(invalid_sales_df)
        assert len(errors) > 0
        # At least one row should survive (the third row is valid)
        assert len(valid_df) <= len(invalid_sales_df)
