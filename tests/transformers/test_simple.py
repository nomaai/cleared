"""Unit tests for simple transformers."""

import unittest
import pandas as pd
from cleared.transformers.simple import ColumnDropper
from cleared.config.structure import IdentifierConfig


class TestColumnDropper(unittest.TestCase):
    """Test the ColumnDropper transformer."""

    def setUp(self):
        """Set up test fixtures."""
        self.idconfig = IdentifierConfig(
            name="patient_id", uid="patient_uid", description="Patient identifier"
        )

        self.test_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "age": [25, 30, 35, 40, 45],
            }
        )

    def test_initialization_success(self):
        """Test successful initialization."""
        transformer = ColumnDropper(idconfig=self.idconfig, uid="test_uid")

        assert transformer.idconfig == self.idconfig
        assert transformer.uid == "test_uid"

    def test_initialization_without_uid(self):
        """Test initialization without uid."""
        transformer = ColumnDropper(idconfig=self.idconfig)

        # BaseTransformer auto-generates a UID when none is provided
        assert transformer.uid is not None

    def test_initialization_with_dependencies(self):
        """Test initialization with dependencies."""
        transformer = ColumnDropper(
            idconfig=self.idconfig, dependencies=["dep1", "dep2"]
        )

        assert transformer.dependencies == ["dep1", "dep2"]

    def test_initialization_none_idconfig(self):
        """Test initialization with None idconfig."""
        with self.assertRaises(ValueError) as context:
            ColumnDropper(idconfig=None)

        assert "idconfig is required for ColumnDropper" in str(context.exception)

    def test_initialization_with_dict_idconfig(self):
        """Test initialization with dict idconfig."""
        idconfig_dict = {
            "name": "test_column",
            "uid": "test_uid",
            "description": "Test column",
        }

        transformer = ColumnDropper(idconfig=idconfig_dict)

        assert transformer.idconfig.name == "test_column"
        assert transformer.idconfig.uid == "test_uid"
        assert transformer.idconfig.description == "Test column"

    def test_initialization_with_nested_dict_idconfig(self):
        """Test initialization with nested dict idconfig."""
        idconfig_dict = {
            "idconfig": {
                "name": "test_column",
                "uid": "test_uid",
                "description": "Test column",
            }
        }

        transformer = ColumnDropper(idconfig=idconfig_dict)

        assert transformer.idconfig.name == "test_column"
        assert transformer.idconfig.uid == "test_uid"
        assert transformer.idconfig.description == "Test column"

    def test_transform_success(self):
        """Test successful transformation."""
        transformer = ColumnDropper(idconfig=self.idconfig)

        result_df, result_deid_ref_dict = transformer.transform(self.test_df, {})

        # Check that the column was dropped
        assert "patient_id" not in result_df.columns
        assert "name" in result_df.columns
        assert "age" in result_df.columns

        # Check that other columns are preserved
        assert len(result_df) == len(self.test_df)
        pd.testing.assert_frame_equal(
            result_df[["name", "age"]], self.test_df[["name", "age"]]
        )

        # Check that deid_ref_dict is unchanged
        assert result_deid_ref_dict == {}

    def test_transform_missing_column(self):
        """Test transformation with missing column."""
        transformer = ColumnDropper(idconfig=self.idconfig)

        # Create DataFrame without the target column
        df_without_column = self.test_df.drop(columns=["patient_id"])

        with self.assertRaises(ValueError) as context:
            transformer.transform(df_without_column, {})

        assert "Column 'patient_id' not found in DataFrame" in str(context.exception)

    def test_transform_empty_dataframe(self):
        """Test transformation with empty DataFrame."""
        transformer = ColumnDropper(idconfig=self.idconfig)

        empty_df = pd.DataFrame(columns=["patient_id", "name", "age"])

        result_df, _ = transformer.transform(empty_df, {})

        # Should succeed and return empty DataFrame without the dropped column
        assert "patient_id" not in result_df.columns
        assert "name" in result_df.columns
        assert "age" in result_df.columns
        assert len(result_df) == 0

    def test_transform_preserves_deid_ref_dict(self):
        """Test that deid_ref_dict is preserved and copied."""
        transformer = ColumnDropper(idconfig=self.idconfig)

        original_deid_ref_dict = {"test_key": pd.DataFrame({"col1": [1, 2, 3]})}

        _, result_deid_ref_dict = transformer.transform(
            self.test_df, original_deid_ref_dict
        )

        # Check that deid_ref_dict is copied (not the same object)
        assert result_deid_ref_dict is not original_deid_ref_dict

        # Check that content is preserved
        assert "test_key" in result_deid_ref_dict
        pd.testing.assert_frame_equal(
            result_deid_ref_dict["test_key"], original_deid_ref_dict["test_key"]
        )

    def test_transform_single_column_dataframe(self):
        """Test transformation with DataFrame containing only the target column."""
        transformer = ColumnDropper(idconfig=self.idconfig)

        single_col_df = pd.DataFrame({"patient_id": [1, 2, 3]})

        result_df, _ = transformer.transform(single_col_df, {})

        # Should return empty DataFrame with no columns
        assert len(result_df.columns) == 0
        assert len(result_df) == 3  # Rows should be preserved

    def test_transform_multiple_columns_same_name(self):
        """Test transformation with multiple columns having similar names."""
        transformer = ColumnDropper(idconfig=self.idconfig)

        # Create DataFrame with similar column names
        df_with_similar_cols = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "patient_id_backup": [1, 2, 3],
                "patient_id_old": [1, 2, 3],
            }
        )

        result_df, _ = transformer.transform(df_with_similar_cols, {})

        # Only the exact match should be dropped
        assert "patient_id" not in result_df.columns
        assert "patient_id_backup" in result_df.columns
        assert "patient_id_old" in result_df.columns

    def test_transform_with_none_deid_ref_dict(self):
        """Test transformation with None deid_ref_dict."""
        transformer = ColumnDropper(idconfig=self.idconfig)

        result_df, result_deid_ref_dict = transformer.transform(self.test_df, None)

        # Should handle None gracefully
        assert result_deid_ref_dict is None
        assert "patient_id" not in result_df.columns


if __name__ == "__main__":
    unittest.main()
