"""Tests for FilterableTransformer."""

from __future__ import annotations

import pandas as pd
import pytest
from cleared.transformers.base import FilterableTransformer
from cleared.config.structure import FilterConfig


class MockFilterableTransformer(FilterableTransformer):
    """Mock implementation of FilterableTransformer for testing."""

    def _apply_transform(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Apply a simple transformation: modify existing data."""
        df = df.copy()
        # Simple transformation: multiply age by 2
        df["age"] = df["age"] * 2
        return df, deid_ref_dict

    def _apply_reverse(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Apply reverse transformation: divide age by 2."""
        df = df.copy()
        # Reverse transformation: divide age by 2
        if "age" in df.columns:
            df["age"] = df["age"] / 2
        return df, deid_ref_dict


class TestFilterableTransformerBasic:
    """Test basic functionality of FilterableTransformer."""

    def test_init_without_filter_config(self):
        """Test initialization without filter config."""
        transformer = MockFilterableTransformer()
        assert transformer.filter_config is None
        assert transformer._original_index is None
        assert transformer._filtered_indices is None

    def test_init_with_filter_config(self):
        """Test initialization with filter config."""
        filter_config = FilterConfig(where_condition="age > 25")
        transformer = MockFilterableTransformer(filter_config=filter_config)
        assert transformer.filter_config == filter_config

    def test_apply_filters_no_filter(self):
        """Test apply_filters when no filter is configured."""
        transformer = MockFilterableTransformer()
        df = pd.DataFrame({"age": [20, 30, 40], "name": ["A", "B", "C"]})

        result = transformer.apply_filters(df)
        pd.testing.assert_frame_equal(result, df)

    def test_apply_filters_with_condition(self):
        """Test apply_filters with a WHERE condition."""
        filter_config = FilterConfig(where_condition="age > 25")
        transformer = MockFilterableTransformer(filter_config=filter_config)
        df = pd.DataFrame({"age": [20, 30, 40], "name": ["A", "B", "C"]})

        result = transformer.apply_filters(df)
        expected = pd.DataFrame({"age": [30, 40], "name": ["B", "C"]}, index=[1, 2])
        pd.testing.assert_frame_equal(result, expected)

    def test_apply_filters_invalid_condition(self):
        """Test apply_filters with invalid WHERE condition."""
        filter_config = FilterConfig(where_condition="invalid_column > 25")
        transformer = MockFilterableTransformer(filter_config=filter_config)
        df = pd.DataFrame({"age": [20, 30, 40], "name": ["A", "B", "C"]})

        with pytest.raises(RuntimeError, match="Invalid filter condition"):
            transformer.apply_filters(df)

    def test_undo_filters_no_filter(self):
        """Test undo_filters when no filter was applied."""
        transformer = MockFilterableTransformer()
        original_df = pd.DataFrame({"age": [20, 30, 40], "name": ["A", "B", "C"]})
        transformed_df = pd.DataFrame({"age": [40, 60, 80], "name": ["A", "B", "C"]})

        result = transformer.undo_filters(original_df, transformed_df)
        pd.testing.assert_frame_equal(result, transformed_df)

    def test_undo_filters_with_filter(self):
        """Test undo_filters when filter was applied."""
        filter_config = FilterConfig(where_condition="age > 25")
        transformer = MockFilterableTransformer(filter_config=filter_config)

        # Simulate the state after apply_filters
        transformer._filtered_indices = pd.Index([1, 2])

        original_df = pd.DataFrame({"age": [20, 30, 40], "name": ["A", "B", "C"]})
        transformed_df = pd.DataFrame(
            {"age": [60, 80], "name": ["B", "C"]}, index=[1, 2]
        )

        result = transformer.undo_filters(original_df, transformed_df)

        expected = pd.DataFrame({"age": [20, 60, 80], "name": ["A", "B", "C"]})
        pd.testing.assert_frame_equal(result, expected)

    def test_full_transform_workflow(self):
        """Test the complete transform workflow."""
        filter_config = FilterConfig(where_condition="age > 25")
        transformer = MockFilterableTransformer(filter_config=filter_config)

        df = pd.DataFrame({"age": [20, 30, 40], "name": ["A", "B", "C"]})
        deid_ref_dict = {}

        result_df, result_deid_ref_dict = transformer.transform(df, deid_ref_dict)

        # Check that the result has the same number of rows as original
        assert len(result_df) == len(df)

        # Check that only filtered rows have their age multiplied by 2
        expected = pd.DataFrame({"age": [20, 60, 80], "name": ["A", "B", "C"]})
        pd.testing.assert_frame_equal(result_df, expected)

        # Check that deid_ref_dict is returned unchanged
        assert result_deid_ref_dict == deid_ref_dict

    def test_full_transform_workflow_no_filter(self):
        """Test the complete transform workflow without filter."""
        transformer = MockFilterableTransformer()

        df = pd.DataFrame({"age": [20, 30, 40], "name": ["A", "B", "C"]})
        deid_ref_dict = {}

        result_df, _ = transformer.transform(df, deid_ref_dict)

        # Check that all rows have their age multiplied by 2
        expected = pd.DataFrame({"age": [40, 60, 80], "name": ["A", "B", "C"]})
        pd.testing.assert_frame_equal(result_df, expected)

    def test_complex_filter_condition(self):
        """Test with a more complex filter condition."""
        filter_config = FilterConfig(where_condition="age > 25 and name in ['B', 'C']")
        transformer = MockFilterableTransformer(filter_config=filter_config)

        df = pd.DataFrame({"age": [20, 30, 40, 35], "name": ["A", "B", "C", "D"]})
        deid_ref_dict = {}

        result_df, _ = transformer.transform(df, deid_ref_dict)

        # Only rows with age > 25 AND name in ['B', 'C'] should have their age multiplied by 2
        expected = pd.DataFrame({"age": [20, 60, 80, 35], "name": ["A", "B", "C", "D"]})
        pd.testing.assert_frame_equal(result_df, expected)


class TestFilterConfig:
    """Test FilterConfig dataclass."""

    def test_init_with_where_condition(self):
        """Test initialization with where condition."""
        config = FilterConfig(where_condition="age > 25")
        assert config.where_condition == "age > 25"
        assert config.description is None

    def test_init_with_description(self):
        """Test initialization with description."""
        config = FilterConfig(
            where_condition="age > 25", description="Filter for adults"
        )
        assert config.where_condition == "age > 25"
        assert config.description == "Filter for adults"

    def test_validation_empty_condition(self):
        """Test validation with empty condition."""
        with pytest.raises(
            ValueError, match="where_condition must be a non-empty string"
        ):
            FilterConfig(where_condition="")

    def test_validation_none_condition(self):
        """Test validation with None condition."""
        with pytest.raises(
            ValueError, match="where_condition must be a non-empty string"
        ):
            FilterConfig(where_condition=None)

    def test_validation_non_string_condition(self):
        """Test validation with non-string condition."""
        with pytest.raises(
            ValueError, match="where_condition must be a non-empty string"
        ):
            FilterConfig(where_condition=123)
