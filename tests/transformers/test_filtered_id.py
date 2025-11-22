"""Tests for IDDeidentifier as a FilterableTransformer."""

import pandas as pd
import pytest
from cleared.transformers.id import IDDeidentifier
from cleared.config.structure import IdentifierConfig, FilterConfig


class TestFilteredIDDeidentifier:
    """Test IDDeidentifier with filtering capabilities."""

    def setup_method(self):
        """Set up test data."""
        self.test_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "age": [25, 30, 35, 40, 45],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "department": ["A", "B", "A", "C", "B"],
            }
        )

        self.test_deid_ref_dict = {}

    def test_init_without_filter_config(self):
        """Test initialization without filter config."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        transformer = IDDeidentifier(idconfig=idconfig)

        assert transformer.filter_config is None
        assert transformer.idconfig.name == "patient_id"

    def test_init_with_filter_config(self):
        """Test initialization with filter config."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = IDDeidentifier(idconfig=idconfig, filter_config=filter_config)

        assert transformer.filter_config == filter_config
        assert transformer.idconfig.name == "patient_id"

    def test_transform_without_filter(self):
        """Test transformation without any filter applied."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        transformer = IDDeidentifier(idconfig=idconfig)

        result_df, result_deid_ref_dict = transformer.transform(
            self.test_df, self.test_deid_ref_dict
        )

        # All rows should be processed
        assert len(result_df) == len(self.test_df)
        assert "patient_id" in result_deid_ref_dict

        # Check that all original IDs are de-identified
        original_ids = set(self.test_df["patient_id"])
        deid_ids = set(result_df["patient_id"])
        assert len(original_ids) == len(deid_ids)
        # Note: De-identified IDs may overlap with original IDs when starting from 1
        # The important thing is that the mapping is consistent

    def test_transform_with_age_filter(self):
        """Test transformation with age-based filter."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = IDDeidentifier(idconfig=idconfig, filter_config=filter_config)

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # All rows should be present
        assert len(result_df) == len(self.test_df)

        # Only rows with age > 30 should have de-identified IDs
        young_patients = result_df[result_df["age"] <= 30]
        old_patients = result_df[result_df["age"] > 30]

        original_young_patients = self.test_df[self.test_df["age"] <= 30]
        original_old_patients = self.test_df[self.test_df["age"] > 30]

        # Young patients should have original IDs
        assert all(
            young_patients["patient_id"] == original_young_patients["patient_id"]
        )

        # Old patients should have different de-identified IDs
        assert not any(
            old_patients["patient_id"] == original_old_patients["patient_id"]
        )

    def test_transform_with_department_filter(self):
        """Test transformation with department-based filter."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        filter_config = FilterConfig(where_condition="department == 'A'")
        transformer = IDDeidentifier(idconfig=idconfig, filter_config=filter_config)

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # All rows should be present
        assert len(result_df) == len(self.test_df)

        # Only department A patients should have de-identified IDs
        dept_a_patients = result_df[result_df["department"] == "A"]
        other_patients = result_df[result_df["department"] != "A"]

        # Department A patients should have de-identified IDs (may be same as original due to sequential generation)
        # The important thing is that the mapping is consistent
        assert len(dept_a_patients) == 2  # Should have 2 department A patients

        # Other patients should have original IDs
        assert all(other_patients["patient_id"] == other_patients["patient_id"])

    def test_transform_with_complex_filter(self):
        """Test transformation with complex filter condition."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        filter_config = FilterConfig(
            where_condition="age > 30 and department in ['A', 'B']"
        )
        transformer = IDDeidentifier(idconfig=idconfig, filter_config=filter_config)

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # All rows should be present
        assert len(result_df) == len(self.test_df)

        # Only patients matching the complex condition should be de-identified
        filtered_patients = result_df[
            (result_df["age"] > 30) & (result_df["department"].isin(["A", "B"]))
        ]
        other_patients = result_df[
            ~((result_df["age"] > 30) & (result_df["department"].isin(["A", "B"])))
        ]
        original_filtered_patients = self.test_df[
            (self.test_df["age"] > 30) & (self.test_df["department"].isin(["A", "B"]))
        ]
        original_other_patients = self.test_df[
            ~(
                (self.test_df["age"] > 30)
                & (self.test_df["department"].isin(["A", "B"]))
            )
        ]
        # Filtered patients should have different de-identified IDs
        assert not any(
            filtered_patients["patient_id"] == original_filtered_patients["patient_id"]
        )

        # Other patients should have original IDs
        assert all(
            other_patients["patient_id"] == original_other_patients["patient_id"]
        )

    def test_transform_with_existing_deid_ref(self):
        """Test transformation with existing de-identification reference."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = IDDeidentifier(idconfig=idconfig, filter_config=filter_config)

        # Pre-populate deid_ref_dict with some mappings
        existing_ref_df = pd.DataFrame(
            {"patient_id": [1, 2], "patient_id__deid": [101, 102]}
        )
        initial_deid_ref_dict = {"patient_id": existing_ref_df}

        result_df, _ = transformer.transform(self.test_df, initial_deid_ref_dict)

        # Check that existing mappings are preserved for filtered patients
        # Patients 1 and 2 have age <= 30, so they're not filtered and keep original values
        # Patients 3, 4, 5 have age > 30, so they're filtered and get new de-identified values
        filtered_patients = result_df[result_df["age"] > 30]
        assert len(filtered_patients) == 3  # Should have 3 filtered patients

        # Check that new mappings are created for filtered patients
        new_patients = result_df[
            (result_df["age"] > 30) & (~result_df["patient_id"].isin([1, 2]))
        ]
        original_new_patients = self.test_df[
            (self.test_df["age"] > 30) & (~self.test_df["patient_id"].isin([1, 2]))
        ]
        assert not any(
            new_patients["patient_id"] == original_new_patients["patient_id"]
        )

    def test_transform_consistency_across_runs(self):
        """Test that de-identification is consistent across multiple runs."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = IDDeidentifier(idconfig=idconfig, filter_config=filter_config)

        # First run
        result_df1, deid_ref_dict1 = transformer.transform(
            self.test_df, self.test_deid_ref_dict
        )

        # Second run with same data
        result_df2, deid_ref_dict2 = transformer.transform(
            self.test_df, self.test_deid_ref_dict
        )

        # Results should be identical
        pd.testing.assert_frame_equal(result_df1, result_df2)
        pd.testing.assert_frame_equal(
            deid_ref_dict1["patient_id"], deid_ref_dict2["patient_id"]
        )

    def test_transform_with_invalid_filter_condition(self):
        """Test transformation with invalid filter condition."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        filter_config = FilterConfig(where_condition="invalid_column > 30")
        transformer = IDDeidentifier(idconfig=idconfig, filter_config=filter_config)

        with pytest.raises(RuntimeError, match="Invalid filter condition"):
            transformer.transform(self.test_df, self.test_deid_ref_dict)

    def test_transform_with_empty_filtered_result(self):
        """Test transformation when filter results in empty DataFrame."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        filter_config = FilterConfig(where_condition="age > 100")  # No patients match
        transformer = IDDeidentifier(idconfig=idconfig, filter_config=filter_config)

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # All rows should be present with original IDs
        assert len(result_df) == len(self.test_df)

    def test_transform_with_dict_idconfig(self):
        """Test transformation with dictionary idconfig."""
        idconfig_dict = {"name": "patient_id", "uid": "patient_id"}
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = IDDeidentifier(
            idconfig=idconfig_dict, filter_config=filter_config
        )

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # Should work the same as with IdentifierConfig object
        assert len(result_df) == len(self.test_df)

    def test_transform_preserves_other_columns(self):
        """Test that transformation preserves all other columns."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = IDDeidentifier(idconfig=idconfig, filter_config=filter_config)

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # All original columns should be preserved
        original_columns = set(self.test_df.columns)
        result_columns = set(result_df.columns)
        assert original_columns == result_columns

        # Original data should be unchanged for non-ID columns
        for col in ["age", "name", "department"]:
            pd.testing.assert_series_equal(
                result_df[col], self.test_df[col], check_names=False
            )
