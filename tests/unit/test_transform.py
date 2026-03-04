"""
Unit tests for transform.py — Data cleaning and transformation.
"""
import pandas as pd
from etl.transform import transform_dataframe


class TestTransformDataframe:
    """Tests for the transform_dataframe function."""

    def test_whitespace_stripped(self, valid_sales_df):
        """Leading/trailing whitespace should be removed from strings."""
        valid_sales_df.loc[0, "product"] = "  Widget Pro  "
        valid_sales_df.loc[0, "region"] = " North America "
        result = transform_dataframe(valid_sales_df.copy())
        assert result.loc[0, "product"] == "Widget Pro"

    def test_total_amount_computed(self, valid_sales_df):
        """total_amount should equal quantity * unit_price."""
        result = transform_dataframe(valid_sales_df.copy())
        assert "total_amount" in result.columns
        for _, row in result.iterrows():
            expected = round(row["quantity"] * row["unit_price"], 2)
            assert row["total_amount"] == expected

    def test_duplicates_removed(self, valid_sales_df):
        """Duplicate order_ids should be de-duplicated (keep last)."""
        dup = valid_sales_df.copy()
        dup = pd.concat([dup, dup.iloc[[0]]], ignore_index=True)
        dup.loc[len(dup) - 1, "quantity"] = 999
        result = transform_dataframe(dup)
        matching = result[result["order_id"] == valid_sales_df.loc[0, "order_id"]]
        assert len(matching) == 1
        assert matching.iloc[0]["quantity"] == 999  # kept last

    def test_region_normalized(self, valid_sales_df):
        """Region casing should be normalized to title case."""
        valid_sales_df.loc[0, "region"] = "NORTH AMERICA"
        valid_sales_df.loc[1, "region"] = "europe"
        result = transform_dataframe(valid_sales_df.copy())
        assert result.loc[0, "region"] == "North America"
        assert result.loc[1, "region"] == "Europe"

    def test_empty_dataframe(self):
        """Empty DataFrame should return empty without errors."""
        empty = pd.DataFrame(columns=[
            "order_id", "order_date", "customer_id", "region",
            "product", "quantity", "unit_price",
        ])
        result = transform_dataframe(empty)
        assert result.empty
