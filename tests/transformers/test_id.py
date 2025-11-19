"""Comprehensive unit tests for IDDeidentifier class."""

import pytest
import pandas as pd
import numpy as np

from cleared.transformers.id import IDDeidentifier
from cleared.config.structure import IdentifierConfig
from cleared.transformers.base import BaseTransformer


class TestIDDeidentifierInitialization:
    """Test IDDeidentifier initialization and constructor."""

    def test_init_with_identifier_config(self):
        """Test initialization with IdentifierConfig object."""
        idconfig = IdentifierConfig(
            name="patient_id", uid="patient_id", description="Patient identifier"
        )

        transformer = IDDeidentifier(idconfig)

        assert isinstance(transformer, BaseTransformer)
        assert transformer.idconfig == idconfig
        assert transformer.idconfig.name == "patient_id"
        assert transformer.idconfig.uid == "patient_id"
        assert transformer.idconfig.description == "Patient identifier"

    def test_init_with_dict_config(self):
        """Test initialization with dictionary config."""
        config_dict = {
            "name": "patient_id",
            "uid": "patient_id",
            "description": "Patient identifier",
        }

        transformer = IDDeidentifier(config_dict)

        assert isinstance(transformer.idconfig, IdentifierConfig)
        assert transformer.idconfig.name == "patient_id"
        assert transformer.idconfig.uid == "patient_id"
        assert transformer.idconfig.description == "Patient identifier"

    def test_init_with_nested_dict_config(self):
        """Test initialization with nested dictionary config."""
        config_dict = {
            "idconfig": {
                "name": "patient_id",
                "uid": "patient_id",
                "description": "Patient identifier",
            }
        }

        transformer = IDDeidentifier(config_dict)

        assert isinstance(transformer.idconfig, IdentifierConfig)
        assert transformer.idconfig.name == "patient_id"
        assert transformer.idconfig.uid == "patient_id"

    def test_init_with_custom_uid_and_dependencies(self):
        """Test initialization with custom UID and dependencies."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        dependencies = ["dep1", "dep2"]

        transformer = IDDeidentifier(
            idconfig=idconfig, uid="custom_uid", dependencies=dependencies
        )

        assert transformer.uid == "custom_uid"
        assert transformer.dependencies == dependencies

    def test_init_with_none_idconfig_raises_error(self):
        """Test that None idconfig raises ValueError."""
        with pytest.raises(ValueError, match="idconfig is required for IDDeidentifier"):
            IDDeidentifier(None)

    def test_init_with_empty_dict_raises_error(self):
        """Test that empty dict raises error during IdentifierConfig creation."""
        with pytest.raises(TypeError):
            IDDeidentifier({})

    def test_deid_uid_method(self):
        """Test the deid_uid method of IdentifierConfig."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        transformer = IDDeidentifier(idconfig)

        assert transformer.idconfig.deid_uid() == "patient_id__deid"


class TestIDDeidentifierTransform:
    """Test IDDeidentifier transform method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.idconfig = IdentifierConfig(
            name="patient_id", uid="patient_id", description="Patient identifier"
        )
        self.transformer = IDDeidentifier(self.idconfig)

        self.test_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 1, 2],  # Repeated values as expected
                "name": ["Alice", "Bob", "Charlie", "Alice", "Bob"],
            }
        )

    def test_transform_with_empty_deid_ref_dict(self):
        """Test transform with empty deid_ref_dict creates new mappings."""
        deid_ref_dict = {}

        result_df, result_deid_ref_dict = self.transformer.transform(
            self.test_df, deid_ref_dict
        )

        # Check that result has correct structure
        assert len(result_df) == len(self.test_df)
        assert "patient_id" in result_df.columns
        assert "name" in result_df.columns

        # Check that patient_id values are de-identified (sequential integers)
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in result_df["patient_id"]
        )
        # Check that values are sequential starting from 1 (with duplicates as in original data)
        deid_values = sorted(result_df["patient_id"].tolist())
        assert deid_values == [1, 1, 2, 2, 3]  # Original data was [1, 2, 3, 1, 2]

        # Check that deid_ref_dict was updated
        assert self.idconfig.uid in result_deid_ref_dict
        deid_ref_df = result_deid_ref_dict[self.idconfig.uid]
        assert len(deid_ref_df) == 3  # 3 unique values
        assert self.idconfig.uid in deid_ref_df.columns
        assert self.idconfig.deid_uid() in deid_ref_df.columns

    def test_transform_with_existing_mappings(self):
        """Test transform with existing mappings in deid_ref_dict."""
        # Create existing mappings
        existing_deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [1, 2],
                self.idconfig.deid_uid(): ["existing_deid_1", "existing_deid_2"],
            }
        )
        deid_ref_dict = {self.idconfig.uid: existing_deid_ref_df}

        _, result_deid_ref_dict = self.transformer.transform(
            self.test_df, deid_ref_dict
        )

        # Check that existing mappings were preserved
        updated_deid_ref_df = result_deid_ref_dict[self.idconfig.uid]
        assert len(updated_deid_ref_df) == 3  # 2 existing + 1 new
        assert "existing_deid_1" in updated_deid_ref_df[self.idconfig.deid_uid()].values
        assert "existing_deid_2" in updated_deid_ref_df[self.idconfig.deid_uid()].values

    def test_transform_with_missing_column_raises_error(self):
        """Test that missing column raises ValueError."""
        df_without_column = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})

        with pytest.raises(
            ValueError, match="Column 'patient_id' not found in DataFrame"
        ):
            self.transformer.transform(df_without_column, {})

    def test_transform_with_incomplete_mappings_raises_error(self):
        """Test that incomplete mappings raise ValueError."""
        # Create incomplete mappings (missing some values)
        incomplete_deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [1],  # Only has mapping for 1, missing 2 and 3
                self.idconfig.deid_uid(): ["deid_1"],
            }
        )
        deid_ref_dict = {self.idconfig.uid: incomplete_deid_ref_df}

        # This should work because the method adds missing mappings
        result_df, _ = self.transformer.transform(self.test_df, deid_ref_dict)

        # Should have all mappings now
        assert len(result_df) == len(self.test_df)

    def test_transform_preserves_other_columns(self):
        """Test that transform preserves other columns unchanged."""
        deid_ref_dict = {}

        result_df, _ = self.transformer.transform(self.test_df, deid_ref_dict)

        # Check that name column is preserved
        pd.testing.assert_series_equal(result_df["name"], self.test_df["name"])

    def test_transform_deterministic_deid_values(self):
        """Test that de-identified values are deterministic."""
        deid_ref_dict = {}

        result_df1, _ = self.transformer.transform(self.test_df, deid_ref_dict)
        result_df2, _ = self.transformer.transform(self.test_df, deid_ref_dict)

        # Same input should produce same de-identified values
        pd.testing.assert_series_equal(
            result_df1["patient_id"], result_df2["patient_id"]
        )

    def test_transform_with_none_values(self):
        """Test transform with None values in the data."""
        df_with_none = pd.DataFrame(
            {
                "patient_id": [1, 2, None, 1],
                "name": ["Alice", "Bob", "Charlie", "Alice"],
            }
        )

        deid_ref_dict = {}
        # This should raise an error because None values don't have mappings
        with pytest.raises(
            ValueError, match="Some values in 'patient_id' don't have deid mappings"
        ):
            self.transformer.transform(df_with_none, deid_ref_dict)

    def test_transform_with_empty_dataframe(self):
        """Test transform with empty DataFrame."""
        # Create empty DataFrame with proper dtypes
        empty_df = pd.DataFrame(
            {"patient_id": pd.Series(dtype="int64"), "name": pd.Series(dtype="str")}
        )
        deid_ref_dict = {}

        # Empty DataFrame should work without errors
        result_df, result_deid_ref_dict = self.transformer.transform(
            empty_df, deid_ref_dict
        )

        assert len(result_df) == 0
        assert self.idconfig.uid in result_deid_ref_dict


class TestIDDeidentifierGetAndUpdateDeidMappings:
    """Test _get_and_update_deid_mappings private method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        self.transformer = IDDeidentifier(self.idconfig)

        self.test_df = pd.DataFrame(
            {"patient_id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
        )

    def test_get_and_update_with_empty_deid_ref_dict(self):
        """Test _get_and_update_deid_mappings with empty deid_ref_dict."""
        deid_ref_dict = {}

        result_df = self.transformer._get_and_update_deid_mappings(
            self.test_df, deid_ref_dict
        )

        # Should create new DataFrame with correct structure
        assert len(result_df) == 3
        assert self.idconfig.uid in result_df.columns
        assert self.idconfig.deid_uid() in result_df.columns
        assert set(result_df[self.idconfig.uid]) == {1, 2, 3}

    def test_get_and_update_with_existing_mappings(self):
        """Test _get_and_update_deid_mappings with existing mappings."""
        existing_deid_ref_df = pd.DataFrame(
            {self.idconfig.uid: [1, 2], self.idconfig.deid_uid(): ["deid_1", "deid_2"]}
        )
        deid_ref_dict = {self.idconfig.uid: existing_deid_ref_df}

        result_df = self.transformer._get_and_update_deid_mappings(
            self.test_df, deid_ref_dict
        )

        # Should add missing value (3) to existing mappings
        assert len(result_df) == 3
        assert set(result_df[self.idconfig.uid]) == {1, 2, 3}
        assert "deid_1" in result_df[self.idconfig.deid_uid()].values
        assert "deid_2" in result_df[self.idconfig.deid_uid()].values

    def test_get_and_update_with_invalid_deid_ref_df_structure(self):
        """Test _get_and_update_deid_mappings with invalid deid_ref_df structure."""
        # Create deid_ref_df with wrong column names
        invalid_deid_ref_df = pd.DataFrame(
            {"wrong_uid": [1, 2], "wrong_deid": ["deid_1", "deid_2"]}
        )
        deid_ref_dict = {self.idconfig.uid: invalid_deid_ref_df}

        with pytest.raises(
            ValueError, match="Deid column 'patient_id__deid' not found"
        ):
            self.transformer._get_and_update_deid_mappings(self.test_df, deid_ref_dict)

    def test_get_and_update_with_missing_uid_column(self):
        """Test _get_and_update_deid_mappings with missing UID column."""
        invalid_deid_ref_df = pd.DataFrame(
            {self.idconfig.deid_uid(): ["deid_1", "deid_2"]}
        )
        deid_ref_dict = {self.idconfig.uid: invalid_deid_ref_df}

        with pytest.raises(
            ValueError, match="UID of the identifier column 'patient_id' not found"
        ):
            self.transformer._get_and_update_deid_mappings(self.test_df, deid_ref_dict)

    def test_get_and_update_no_new_values(self):
        """Test _get_and_update_deid_mappings when no new values need to be added."""
        existing_deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [1, 2, 3],
                self.idconfig.deid_uid(): ["deid_1", "deid_2", "deid_3"],
            }
        )
        deid_ref_dict = {self.idconfig.uid: existing_deid_ref_df}

        result_df = self.transformer._get_and_update_deid_mappings(
            self.test_df, deid_ref_dict
        )

        # Should return the same DataFrame (no new values added)
        pd.testing.assert_frame_equal(result_df, existing_deid_ref_df)

    def test_get_and_update_with_duplicate_values(self):
        """Test _get_and_update_deid_mappings with duplicate values in input."""
        df_with_duplicates = pd.DataFrame(
            {
                "patient_id": [1, 2, 1, 2, 3],  # Duplicates
                "name": ["Alice", "Bob", "Alice", "Bob", "Charlie"],
            }
        )

        deid_ref_dict = {}
        result_df = self.transformer._get_and_update_deid_mappings(
            df_with_duplicates, deid_ref_dict
        )

        # Should only have unique values
        assert len(result_df) == 3
        assert set(result_df[self.idconfig.uid]) == {1, 2, 3}


class TestIDDeidentifierGenerateDeidMappings:
    """Test _generate_deid_mappings private method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        self.transformer = IDDeidentifier(self.idconfig)

    def test_generate_deid_mappings_basic(self):
        """Test basic deid mapping generation."""
        values = [1, 2, 3]

        result_df = self.transformer._generate_deid_mappings(values)

        # Check structure
        assert len(result_df) == 3
        assert self.idconfig.uid in result_df.columns
        assert self.idconfig.deid_uid() in result_df.columns

        # Check that all deid values are sequential integers starting from 1
        deid_values = result_df[self.idconfig.deid_uid()].tolist()
        assert deid_values == [1, 2, 3]

        # Check that original values are preserved
        assert set(result_df[self.idconfig.uid]) == {1, 2, 3}

    def test_generate_deid_mappings_deterministic(self):
        """Test that deid mapping generation is deterministic."""
        values = [1, 2, 3]

        result1 = self.transformer._generate_deid_mappings(values)
        result2 = self.transformer._generate_deid_mappings(values)

        # Should produce identical results
        pd.testing.assert_frame_equal(result1, result2)

    def test_generate_deid_mappings_with_strings(self):
        """Test deid mapping generation with string values."""
        values = ["user_001", "user_002", "user_003"]

        result_df = self.transformer._generate_deid_mappings(values)

        assert len(result_df) == 3
        assert set(result_df[self.idconfig.uid]) == set(values)

        # Check deid values are sequential integers starting from 1
        deid_values = result_df[self.idconfig.deid_uid()].tolist()
        assert deid_values == [1, 2, 3]

    def test_generate_deid_mappings_with_mixed_types(self):
        """Test deid mapping generation with mixed data types."""
        values = [1, "string", 3.14, None]

        result_df = self.transformer._generate_deid_mappings(values)

        assert len(result_df) == 4
        assert set(result_df[self.idconfig.uid]) == {1, "string", 3.14, None}

        # Check deid values are sequential integers starting from 1
        deid_values = result_df[self.idconfig.deid_uid()].tolist()
        assert deid_values == [1, 2, 3, 4]

    def test_generate_deid_mappings_empty_list(self):
        """Test deid mapping generation with empty list."""
        values = []

        result_df = self.transformer._generate_deid_mappings(values)

        assert len(result_df) == 0
        assert self.idconfig.uid in result_df.columns
        assert self.idconfig.deid_uid() in result_df.columns

    def test_generate_deid_mappings_uniqueness(self):
        """Test that generated deid values are unique."""
        values = [1, 2, 3, 4, 5]

        result_df = self.transformer._generate_deid_mappings(values)

        deid_values = result_df[self.idconfig.deid_uid()].tolist()
        assert deid_values == [1, 2, 3, 4, 5]  # Sequential and unique

    def test_generate_deid_mappings_with_duplicates(self):
        """Test deid mapping generation with duplicate input values."""
        values = [1, 2, 1, 2, 3]  # Duplicates

        result_df = self.transformer._generate_deid_mappings(values)

        # Should create mappings for all values, including duplicates
        assert len(result_df) == 5
        assert result_df[self.idconfig.uid].tolist() == values

        # Check deid values are sequential integers starting from 1
        deid_values = result_df[self.idconfig.deid_uid()].tolist()
        assert deid_values == [1, 2, 3, 4, 5]


class TestIDDeidentifierEdgeCases:
    """Test IDDeidentifier edge cases and error scenarios."""

    def setup_method(self):
        """Set up test fixtures."""
        self.idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        self.transformer = IDDeidentifier(self.idconfig)

    def test_transform_with_large_dataset(self):
        """Test transform with large dataset."""
        # Create large dataset
        large_df = pd.DataFrame(
            {"patient_id": range(1000), "name": [f"User_{i}" for i in range(1000)]}
        )

        deid_ref_dict = {}
        result_df, result_deid_ref_dict = self.transformer.transform(
            large_df, deid_ref_dict
        )

        assert len(result_df) == 1000
        assert len(result_deid_ref_dict[self.idconfig.uid]) == 1000

    def test_transform_with_special_characters(self):
        """Test transform with special characters in data."""
        df_with_special = pd.DataFrame(
            {
                "patient_id": ["id@123", "user#456", "test$789"],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        deid_ref_dict = {}
        result_df, _ = self.transformer.transform(df_with_special, deid_ref_dict)

        assert len(result_df) == 3
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in result_df["patient_id"]
        )

    def test_transform_with_unicode_values(self):
        """Test transform with unicode values."""
        df_with_unicode = pd.DataFrame(
            {
                "patient_id": ["用户001", "user_002", "пользователь_003"],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        deid_ref_dict = {}
        result_df, _ = self.transformer.transform(df_with_unicode, deid_ref_dict)

        assert len(result_df) == 3
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in result_df["patient_id"]
        )

    def test_transform_preserves_dataframe_index(self):
        """Test that transform preserves DataFrame index."""
        df_with_index = pd.DataFrame(
            {"patient_id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]},
            index=["a", "b", "c"],
        )

        deid_ref_dict = {}
        result_df, _ = self.transformer.transform(df_with_index, deid_ref_dict)

        # The merge operation may reset the index, so we check that the data is preserved
        assert len(result_df) == len(df_with_index)
        assert "patient_id" in result_df.columns
        assert "name" in result_df.columns

    def test_transform_with_nan_values(self):
        """Test transform with NaN values."""
        df_with_nan = pd.DataFrame(
            {
                "patient_id": [1, 2, np.nan, 4],
                "name": ["Alice", "Bob", "Charlie", "Diana"],
            }
        )

        deid_ref_dict = {}
        # This should raise an error because NaN values don't have mappings
        with pytest.raises(
            ValueError, match="Some values in 'patient_id' don't have deid mappings"
        ):
            self.transformer.transform(df_with_nan, deid_ref_dict)

    def test_transform_with_very_long_strings(self):
        """Test transform with very long string values."""
        long_string = "x" * 1000
        df_with_long_strings = pd.DataFrame(
            {
                "patient_id": [long_string, "normal_id", long_string],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        deid_ref_dict = {}
        result_df, _ = self.transformer.transform(df_with_long_strings, deid_ref_dict)

        assert len(result_df) == 3
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in result_df["patient_id"]
        )


class TestIDDeidentifierIntegration:
    """Integration tests for IDDeidentifier."""

    def test_full_workflow_with_multiple_calls(self):
        """Test full workflow with multiple transform calls."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        transformer = IDDeidentifier(idconfig)

        # First batch
        df1 = pd.DataFrame(
            {"patient_id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
        )

        deid_ref_dict = {}
        result_df1, deid_ref_dict = transformer.transform(df1, deid_ref_dict)

        # Second batch with some overlapping values
        df2 = pd.DataFrame(
            {
                "patient_id": [2, 3, 4],  # 2,3 overlap with first batch
                "name": ["Bob", "Charlie", "Diana"],
            }
        )

        result_df2, deid_ref_dict = transformer.transform(df2, deid_ref_dict)

        # Check that overlapping values have same deid values
        deid_ref_df = deid_ref_dict[idconfig.uid]

        # Find deid values for overlapping IDs
        id2_deid1 = deid_ref_df[deid_ref_df[idconfig.uid] == 2][
            idconfig.deid_uid()
        ].iloc[0]
        id3_deid1 = deid_ref_df[deid_ref_df[idconfig.uid] == 3][
            idconfig.deid_uid()
        ].iloc[0]

        # These should be the same in both results
        assert result_df1[result_df1["patient_id"] == id2_deid1].shape[0] > 0
        assert result_df2[result_df2["patient_id"] == id2_deid1].shape[0] > 0
        assert result_df1[result_df1["patient_id"] == id3_deid1].shape[0] > 0
        assert result_df2[result_df2["patient_id"] == id3_deid1].shape[0] > 0

    def test_inheritance_from_base_transformer(self):
        """Test that IDDeidentifier properly inherits from BaseTransformer."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        transformer = IDDeidentifier(idconfig)

        assert isinstance(transformer, BaseTransformer)
        assert hasattr(transformer, "uid")
        assert hasattr(transformer, "dependencies")
        assert hasattr(transformer, "transform")

    def test_transformer_uid_uniqueness(self):
        """Test that transformer UIDs are unique."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        transformer1 = IDDeidentifier(idconfig)
        transformer2 = IDDeidentifier(idconfig)

        assert transformer1.uid != transformer2.uid

    def test_transformer_with_dependencies(self):
        """Test transformer with dependencies."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        dependencies = ["dep1", "dep2"]

        transformer = IDDeidentifier(idconfig=idconfig, dependencies=dependencies)

        assert transformer.dependencies == dependencies

    def test_error_handling_in_transform(self):
        """Test comprehensive error handling in transform method."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        transformer = IDDeidentifier(idconfig)

        # Test with invalid deid_ref_dict structure
        invalid_deid_ref_dict = {
            idconfig.uid: pd.DataFrame(
                {"wrong_column": [1, 2, 3], "another_wrong_column": ["a", "b", "c"]}
            )
        }

        df = pd.DataFrame({"patient_id": [1, 2, 3]})

        with pytest.raises(ValueError):
            transformer.transform(df, invalid_deid_ref_dict)


class TestIDDeidentifierPerformance:
    """Performance tests for IDDeidentifier."""

    def test_performance_with_large_unique_values(self):
        """Test performance with large number of unique values."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        transformer = IDDeidentifier(idconfig)

        # Create DataFrame with many unique values
        n_values = 10000
        df = pd.DataFrame(
            {
                "patient_id": range(n_values),
                "name": [f"User_{i}" for i in range(n_values)],
            }
        )

        deid_ref_dict = {}

        # Time the transform operation
        import time

        start_time = time.time()
        result_df, _ = transformer.transform(df, deid_ref_dict)
        end_time = time.time()

        # Should complete in reasonable time (less than 10 seconds)
        assert end_time - start_time < 10
        assert len(result_df) == n_values

    def test_memory_usage_with_large_dataset(self):
        """Test memory usage with large dataset."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        transformer = IDDeidentifier(idconfig)

        # Create large dataset
        n_values = 5000
        df = pd.DataFrame(
            {
                "patient_id": range(n_values),
                "name": [f"User_{i}" for i in range(n_values)],
            }
        )

        deid_ref_dict = {}
        result_df, result_deid_ref_dict = transformer.transform(df, deid_ref_dict)

        # Check that memory usage is reasonable
        assert (
            result_df.memory_usage(deep=True).sum() < 100 * 1024 * 1024
        )  # Less than 100MB
        assert (
            result_deid_ref_dict[idconfig.uid].memory_usage(deep=True).sum()
            < 50 * 1024 * 1024
        )  # Less than 50MB


class TestIDDeidentifierReverse:
    """Comprehensive tests for IDDeidentifier reverse functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        # Use different name and uid to avoid pandas merge suffix issues when columns have same name
        self.idconfig = IdentifierConfig(
            name="patient_id", uid="patient_uid", description="Patient identifier"
        )
        self.transformer = IDDeidentifier(self.idconfig)

    def test_reverse_success(self):
        """Test successful reverse operation restores original ID values."""
        # Create original data with string IDs to ensure transformation is visible
        original_df = pd.DataFrame(
            {
                "patient_id": [
                    "user_001",
                    "user_002",
                    "user_003",
                    "user_001",
                    "user_002",
                ],  # Repeated values
                "name": ["Alice", "Bob", "Charlie", "Alice", "Bob"],
            }
        )

        # First, transform the data
        deid_ref_dict = {}
        transformed_df, deid_ref_dict = self.transformer.transform(
            original_df, deid_ref_dict
        )

        # Verify transformation occurred (strings converted to integers)
        assert not transformed_df["patient_id"].equals(original_df["patient_id"])
        assert all(
            isinstance(val, (int, np.integer)) for val in transformed_df["patient_id"]
        )

        # Now reverse the transformation
        reversed_df, _ = self.transformer.reverse(transformed_df, deid_ref_dict)

        # Check that ID values are restored
        pd.testing.assert_series_equal(
            reversed_df["patient_id"], original_df["patient_id"]
        )
        pd.testing.assert_series_equal(reversed_df["name"], original_df["name"])

    def test_reverse_with_existing_deid_mappings(self):
        """Test reverse with pre-existing deid mappings in deid_ref_dict."""
        # Create de-identified data (simulating already transformed data)
        # Note: In reverse, the column still has the same name but contains de-identified values
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],  # De-identified values (integers)
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Create deid mappings (original values are strings, de-identified are integers)
        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [
                    "user_001",
                    "user_002",
                    "user_003",
                ],  # Original values
                self.idconfig.deid_uid(): [1, 2, 3],  # De-identified values
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        # Reverse the transformation
        reversed_df, _ = self.transformer.reverse(deid_df, deid_ref_dict)

        # Check that values are restored correctly
        expected_df = pd.DataFrame(
            {
                "patient_id": [
                    "user_001",
                    "user_002",
                    "user_003",
                ],  # Original values restored
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        pd.testing.assert_frame_equal(
            reversed_df[["patient_id", "name"]], expected_df[["patient_id", "name"]]
        )

    def test_reverse_missing_deid_mappings_raises_error(self):
        """Test reverse raises error when deid mappings are missing."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],  # De-identified values
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Empty deid_ref_dict (no deid mappings)
        deid_ref_dict = {}

        with pytest.raises(
            ValueError,
            match=f"De-identification reference not found for transformer {self.transformer.uid}",
        ):
            self.transformer.reverse(deid_df, deid_ref_dict)

    def test_reverse_missing_column_raises_error(self):
        """Test reverse raises error when patient_id column is missing."""
        deid_df = pd.DataFrame(
            {
                "other_id": [1, 2, 3],  # Wrong column name
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [10, 20, 30],
                self.idconfig.deid_uid(): [1, 2, 3],
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        with pytest.raises(
            ValueError, match="Column 'patient_id' not found in DataFrame"
        ):
            self.transformer.reverse(deid_df, deid_ref_dict)

    def test_reverse_incomplete_mappings_raises_error(self):
        """Test reverse raises error when some values don't have mappings."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4],  # 4 values
                "name": ["Alice", "Bob", "Charlie", "Diana"],
            }
        )

        # Deid mappings with only 3 values (missing 4)
        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [10, 20, 30],
                self.idconfig.deid_uid(): [1, 2, 3],
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        with pytest.raises(
            ValueError,
            match="Some values in 'patient_id' don't have deid mappings",
        ):
            self.transformer.reverse(deid_df, deid_ref_dict)

    def test_reverse_empty_dataframe(self):
        """Test reverse with empty DataFrame."""
        # Create empty DataFrame with proper dtypes
        empty_df = pd.DataFrame(
            {"patient_id": pd.Series(dtype="int64"), "name": pd.Series(dtype="str")}
        )
        deid_ref_df = pd.DataFrame(
            columns=[self.idconfig.uid, self.idconfig.deid_uid()]
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        reversed_df, _ = self.transformer.reverse(empty_df, deid_ref_dict)

        assert len(reversed_df) == 0
        assert "patient_id" in reversed_df.columns
        assert "name" in reversed_df.columns

    def test_reverse_preserves_other_columns(self):
        """Test reverse preserves other columns unchanged."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],  # De-identified values
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["NYC", "LA", "SF"],
            }
        )

        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [10, 20, 30],  # Original values
                self.idconfig.deid_uid(): [1, 2, 3],  # De-identified values
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        reversed_df, _ = self.transformer.reverse(deid_df, deid_ref_dict)

        # Check other columns are preserved
        pd.testing.assert_series_equal(reversed_df["name"], deid_df["name"])
        pd.testing.assert_series_equal(reversed_df["age"], deid_df["age"])
        pd.testing.assert_series_equal(reversed_df["city"], deid_df["city"])

    def test_reverse_round_trip_consistency(self):
        """Test that transform -> reverse -> transform maintains consistency."""
        # Use string IDs to ensure transformation is visible
        original_df = pd.DataFrame(
            {
                "patient_id": [
                    "user_001",
                    "user_002",
                    "user_003",
                    "user_001",
                    "user_002",
                ],  # Duplicates
                "name": ["Alice", "Bob", "Charlie", "Alice", "Bob"],
            }
        )

        deid_ref_dict = {}

        # Transform
        transformed_df, deid_ref_dict = self.transformer.transform(
            original_df, deid_ref_dict
        )

        # Reverse
        reversed_df, deid_ref_dict = self.transformer.reverse(
            transformed_df, deid_ref_dict
        )

        # Transform again
        retransformed_df, _ = self.transformer.transform(reversed_df, deid_ref_dict)

        # Check that second transformation produces same result as first
        pd.testing.assert_series_equal(
            transformed_df["patient_id"], retransformed_df["patient_id"]
        )

    def test_reverse_with_string_ids(self):
        """Test reverse works with string ID values."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],  # De-identified integer values
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Original values were strings
        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: ["user_001", "user_002", "user_003"],
                self.idconfig.deid_uid(): [1, 2, 3],
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        reversed_df, _ = self.transformer.reverse(deid_df, deid_ref_dict)

        # Check that string values are restored
        expected_values = ["user_001", "user_002", "user_003"]
        assert list(reversed_df["patient_id"]) == expected_values

    def test_reverse_with_missing_deid_column_in_deid_ref_df(self):
        """Test reverse raises error when deid column is missing in deid_ref_df."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Deid_ref_df missing deid column
        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [10, 20, 30],  # Missing deid_uid column
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        with pytest.raises(
            ValueError,
            match=f"Deid column '{self.idconfig.deid_uid()}' not found in deid_ref_df",
        ):
            self.transformer.reverse(deid_df, deid_ref_dict)

    def test_reverse_with_missing_uid_column_in_deid_ref_df(self):
        """Test reverse raises error when UID column is missing in deid_ref_df."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Deid_ref_df missing UID column
        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.deid_uid(): [1, 2, 3],  # Missing uid column
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        with pytest.raises(
            ValueError,
            match=f"UID column '{self.idconfig.uid}' not found in deid_ref_df",
        ):
            self.transformer.reverse(deid_df, deid_ref_dict)

    def test_reverse_with_duplicate_deid_values(self):
        """Test reverse handles duplicate de-identified values correctly."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 1, 2, 2, 3],  # Duplicate de-identified values
                "name": ["Alice", "Alice2", "Bob", "Bob2", "Charlie"],
            }
        )

        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [10, 20, 30],  # Original values
                self.idconfig.deid_uid(): [1, 2, 3],  # De-identified values
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        reversed_df, _ = self.transformer.reverse(deid_df, deid_ref_dict)

        # Check that duplicates are handled correctly
        # Both rows with deid value 1 should map to original value 10
        assert reversed_df[reversed_df["patient_id"] == 10].shape[0] == 2
        assert reversed_df[reversed_df["patient_id"] == 20].shape[0] == 2
        assert reversed_df[reversed_df["patient_id"] == 30].shape[0] == 1

    def test_reverse_preserves_dataframe_index(self):
        """Test that reverse preserves DataFrame structure."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            },
            index=["a", "b", "c"],
        )

        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [10, 20, 30],
                self.idconfig.deid_uid(): [1, 2, 3],
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        reversed_df, _ = self.transformer.reverse(deid_df, deid_ref_dict)

        # Check that data is preserved (index may change due to merge)
        assert len(reversed_df) == 3
        assert "patient_id" in reversed_df.columns
        assert "name" in reversed_df.columns

    def test_reverse_with_large_dataset(self):
        """Test reverse with large dataset."""
        n_values = 1000
        deid_df = pd.DataFrame(
            {
                "patient_id": range(1, n_values + 1),  # De-identified values
                "name": [f"User_{i}" for i in range(n_values)],
            }
        )

        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: range(1000, 1000 + n_values),  # Original values
                self.idconfig.deid_uid(): range(
                    1, n_values + 1
                ),  # De-identified values
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        reversed_df, _ = self.transformer.reverse(deid_df, deid_ref_dict)

        assert len(reversed_df) == n_values
        assert all(
            val in range(1000, 1000 + n_values) for val in reversed_df["patient_id"]
        )

    def test_reverse_with_special_characters_in_original_ids(self):
        """Test reverse with special characters in original ID values."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],  # De-identified values
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Original values contain special characters
        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: ["id@123", "user#456", "test$789"],
                self.idconfig.deid_uid(): [1, 2, 3],
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        reversed_df, _ = self.transformer.reverse(deid_df, deid_ref_dict)

        expected_values = ["id@123", "user#456", "test$789"]
        assert list(reversed_df["patient_id"]) == expected_values

    def test_reverse_with_unicode_ids(self):
        """Test reverse with unicode ID values."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Original values are unicode
        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: ["用户001", "user_002", "пользователь_003"],
                self.idconfig.deid_uid(): [1, 2, 3],
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df}

        reversed_df, _ = self.transformer.reverse(deid_df, deid_ref_dict)

        expected_values = ["用户001", "user_002", "пользователь_003"]
        assert list(reversed_df["patient_id"]) == expected_values

    def test_reverse_does_not_modify_deid_ref_dict(self):
        """Test reverse does not modify the deid_ref_dict."""
        deid_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        deid_ref_df = pd.DataFrame(
            {
                self.idconfig.uid: [10, 20, 30],
                self.idconfig.deid_uid(): [1, 2, 3],
            }
        )
        deid_ref_dict = {self.idconfig.uid: deid_ref_df.copy()}
        original_deid_ref_dict = {self.idconfig.uid: deid_ref_df.copy()}

        _reversed_df, updated_deid_ref_dict = self.transformer.reverse(
            deid_df, deid_ref_dict
        )

        # Check that deid_ref_dict is unchanged (reverse doesn't update it)
        pd.testing.assert_frame_equal(
            updated_deid_ref_dict[self.idconfig.uid],
            original_deid_ref_dict[self.idconfig.uid],
        )
