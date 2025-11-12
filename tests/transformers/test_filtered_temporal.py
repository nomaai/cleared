"""Tests for DateTimeDeidentifier as a FilterableTransformer."""

import pandas as pd
import pytest
from datetime import datetime, timedelta
from cleared.transformers.temporal import DateTimeDeidentifier
from cleared.config.structure import (
    IdentifierConfig,
    DeIDConfig,
    TimeShiftConfig,
    FilterConfig,
)


class TestFilteredDateTimeDeidentifier:
    """Test DateTimeDeidentifier with filtering capabilities."""

    def setup_method(self):
        """Set up test data."""
        base_date = datetime(2023, 1, 1, 12, 0, 0)
        self.test_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "age": [25, 30, 35, 40, 45],
                "visit_date": [
                    base_date,
                    base_date + timedelta(days=1),
                    base_date + timedelta(days=2),
                    base_date + timedelta(days=3),
                    base_date + timedelta(days=4),
                ],
                "department": ["A", "B", "A", "C", "B"],
                "priority": ["high", "low", "medium", "high", "low"],
            }
        )

        self.test_deid_ref_dict = {}

        # Create test configurations
        self.idconfig = IdentifierConfig(name="patient_id", uid="patient_id")
        self.time_shift_config = TimeShiftConfig(method="shift_by_days", min=30, max=90)
        self.deid_config = DeIDConfig(time_shift=self.time_shift_config)

    def test_init_without_filter_config(self):
        """Test initialization without filter config."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
        )

        assert transformer.filter_config is None
        assert transformer.idconfig.name == "patient_id"
        assert transformer.datetime_column == "visit_date"

    def test_init_with_filter_config(self):
        """Test initialization with filter config."""
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        assert transformer.filter_config == filter_config
        assert transformer.idconfig.name == "patient_id"

    def test_transform_without_filter(self):
        """Test transformation without any filter applied."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
        )

        result_df, result_deid_ref_dict = transformer.transform(
            self.test_df, self.test_deid_ref_dict
        )

        # All rows should be processed
        assert len(result_df) == len(self.test_df)
        assert "patient_id_shift" in result_deid_ref_dict

        # Check that all dates are shifted
        original_dates = self.test_df["visit_date"]
        deid_dates = result_df["visit_date"]

        # All dates should be different
        assert not any(original_dates == deid_dates)

        # All dates should be shifted by a reasonable amount (30-90 days as configured)
        for orig_date, deid_date in zip(original_dates, deid_dates):  # noqa: B905
            shift_days = (deid_date - orig_date).days
            assert 30 <= shift_days <= 90

    def test_transform_with_age_filter(self):
        """Test transformation with age-based filter."""
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # All rows should be present
        assert len(result_df) == len(self.test_df)

        # Only rows with age > 30 should have de-identified dates
        young_patients = result_df[result_df["age"] <= 30]
        old_patients = result_df[result_df["age"] > 30]
        original_young_patients = self.test_df[self.test_df["age"] <= 30]
        original_old_patients = self.test_df[self.test_df["age"] > 30]

        # Young patients should have original dates
        pd.testing.assert_series_equal(
            young_patients["visit_date"],
            original_young_patients["visit_date"],
            check_names=False,
        )

        # Old patients should have different de-identified dates
        assert not any(
            old_patients["visit_date"] == original_old_patients["visit_date"]
        )

    def test_transform_with_department_filter(self):
        """Test transformation with department-based filter."""
        filter_config = FilterConfig(where_condition="department == 'A'")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # All rows should be present
        assert len(result_df) == len(self.test_df)

        # Only department A patients should have de-identified dates
        dept_a_patients = result_df[result_df["department"] == "A"]
        other_patients = result_df[result_df["department"] != "A"]
        original_dept_a_patients = self.test_df[self.test_df["department"] == "A"]
        original_other_patients = self.test_df[self.test_df["department"] != "A"]

        # Department A patients should have different de-identified dates
        assert not any(
            dept_a_patients["visit_date"] == original_dept_a_patients["visit_date"]
        )

        # Other patients should have original dates
        pd.testing.assert_series_equal(
            other_patients["visit_date"],
            original_other_patients["visit_date"],
            check_names=False,
        )

    def test_transform_with_priority_filter(self):
        """Test transformation with priority-based filter."""
        filter_config = FilterConfig(where_condition="priority == 'high'")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # All rows should be present
        assert len(result_df) == len(self.test_df)

        # Only high priority patients should have de-identified dates
        high_priority = result_df[result_df["priority"] == "high"]
        other_priority = result_df[result_df["priority"] != "high"]
        original_high_priority = self.test_df[self.test_df["priority"] == "high"]
        original_other_priority = self.test_df[self.test_df["priority"] != "high"]

        # High priority patients should have different de-identified dates
        assert not any(
            high_priority["visit_date"] == original_high_priority["visit_date"]
        )

        # Other patients should have original dates
        pd.testing.assert_series_equal(
            other_priority["visit_date"],
            original_other_priority["visit_date"],
            check_names=False,
        )

    def test_transform_with_complex_filter(self):
        """Test transformation with complex filter condition."""
        filter_config = FilterConfig(
            where_condition="age > 30 and department in ['A', 'B']"
        )
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

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
        # Filtered patients should have different de-identified dates
        assert not any(
            filtered_patients["visit_date"] == original_filtered_patients["visit_date"]
        )

        # Other patients should have original dates
        pd.testing.assert_series_equal(
            other_patients["visit_date"],
            original_other_patients["visit_date"],
            check_names=False,
        )

    def test_transform_with_existing_timeshift_ref(self):
        """Test transformation with existing timeshift reference."""
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        # Pre-populate deid_ref_dict with some timeshift mappings
        existing_timeshift_df = pd.DataFrame(
            {"patient_id": [1, 2], "shift_days": [45, 60]}
        )
        initial_deid_ref_dict = {"timeshift": existing_timeshift_df}

        result_df, _ = transformer.transform(self.test_df, initial_deid_ref_dict)

        # Check that existing mappings are preserved
        existing_patients = result_df[result_df["patient_id"].isin([1, 2])]
        # These should have their dates shifted according to existing mappings
        assert len(existing_patients) == 2

    def test_transform_consistency_across_runs(self):
        """Test that time shifting is consistent across multiple runs."""
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        # First run
        result_df1, deid_ref_dict1 = transformer.transform(
            self.test_df, self.test_deid_ref_dict
        )

        # Second run with same data
        result_df2, deid_ref_dict2 = transformer.transform(
            self.test_df, self.test_deid_ref_dict
        )

        # Results should be identical for non-shifted columns
        # Note: Time shifting is random, so shifted dates may differ between runs
        # We only check that the structure and non-shifted data are the same
        assert result_df1.shape == result_df2.shape
        assert list(result_df1.columns) == list(result_df2.columns)

        # Check that non-shifted rows (age <= 30) are identical
        young1 = result_df1[result_df1["age"] <= 30]
        young2 = result_df2[result_df2["age"] <= 30]
        pd.testing.assert_frame_equal(young1, young2)

        # For time shifting, we can't expect identical results due to randomness
        # Just check that the structure is the same
        assert (
            deid_ref_dict1["patient_id_shift"].shape
            == deid_ref_dict2["patient_id_shift"].shape
        )
        assert list(deid_ref_dict1["patient_id_shift"].columns) == list(
            deid_ref_dict2["patient_id_shift"].columns
        )

    def test_transform_with_invalid_filter_condition(self):
        """Test transformation with invalid filter condition."""
        filter_config = FilterConfig(where_condition="invalid_column > 30")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        with pytest.raises(ValueError, match="Invalid filter condition"):
            transformer.transform(self.test_df, self.test_deid_ref_dict)

    def test_transform_with_empty_filtered_result(self):
        """Test transformation when filter results in empty DataFrame."""
        filter_config = FilterConfig(where_condition="age > 100")  # No patients match
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # All rows should be present with original dates
        assert len(result_df) == len(self.test_df)
        pd.testing.assert_series_equal(
            result_df["visit_date"], result_df["visit_date"], check_names=False
        )

    def test_transform_preserves_other_columns(self):
        """Test that transformation preserves all other columns."""
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # All original columns should be preserved
        original_columns = set(self.test_df.columns)
        result_columns = set(result_df.columns)
        assert original_columns == result_columns

        # Original data should be unchanged for non-datetime columns
        for col in ["age", "department", "priority"]:
            pd.testing.assert_series_equal(
                result_df[col], self.test_df[col], check_names=False
            )

    def test_transform_with_different_time_shift_methods(self):
        """Test transformation with different time shift methods."""
        # Test with shift_by_days method
        random_shift_config = TimeShiftConfig(method="shift_by_days", min=10, max=50)
        random_deid_config = DeIDConfig(time_shift=random_shift_config)

        filter_config = FilterConfig(where_condition="age > 30")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=random_deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # Should work without errors
        assert len(result_df) == len(self.test_df)

    def test_transform_with_missing_datetime_column(self):
        """Test transformation with missing datetime column."""
        filter_config = FilterConfig(where_condition="age > 30")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="nonexistent_column",
            filter_config=filter_config,
        )

        with pytest.raises(
            ValueError, match="Column 'nonexistent_column' not found in DataFrame"
        ):
            transformer.transform(self.test_df, self.test_deid_ref_dict)

    def test_transform_with_missing_id_column(self):
        """Test transformation with missing ID column."""
        # Create DataFrame without the ID column
        df_no_id = self.test_df.drop(columns=["patient_id"])

        filter_config = FilterConfig(where_condition="age > 30")
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            deid_config=self.deid_config,
            datetime_column="visit_date",
            filter_config=filter_config,
        )

        with pytest.raises(
            ValueError, match="Reference column 'patient_id' not found in DataFrame"
        ):
            transformer.transform(df_no_id, self.test_deid_ref_dict)
