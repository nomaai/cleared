"""
Comprehensive tests for temporal transformers.

This module tests all classes and functions in cleared.transformers.temporal,
including DateTimeDeidentifier, TimeShiftGenerator subclasses, and utility functions.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import patch

from cleared.transformers.temporal import (
    DateTimeDeidentifier,
    TimeShiftGenerator,
    ShiftByHours,
    ShiftByDays,
    ShiftByWeeks,
    ShiftByMonths,
    ShiftByYears,
    _create_time_shift_gen_map,
    create_time_shift_generator,
)
from cleared.config.structure import IdentifierConfig, DeIDConfig, TimeShiftConfig


class TestTimeShiftGenerator:
    """Test the abstract TimeShiftGenerator base class and its concrete implementations."""

    def test_abstract_class_cannot_be_instantiated(self):
        """Test that TimeShiftGenerator cannot be instantiated directly."""
        with pytest.raises(TypeError):
            TimeShiftGenerator(1, 10)

    def test_shift_by_hours_initialization(self):
        """Test ShiftByHours initialization."""
        generator = ShiftByHours(1, 10)
        assert generator.min_value == 1
        assert generator.max_value == 10

    def test_shift_by_days_initialization(self):
        """Test ShiftByDays initialization."""
        generator = ShiftByDays(1, 10)
        assert generator.min_value == 1
        assert generator.max_value == 10

    def test_shift_by_weeks_initialization(self):
        """Test ShiftByWeeks initialization."""
        generator = ShiftByWeeks(1, 10)
        assert generator.min_value == 1
        assert generator.max_value == 10

    def test_shift_by_months_initialization(self):
        """Test ShiftByMonths initialization."""
        generator = ShiftByMonths(1, 10)
        assert generator.min_value == 1
        assert generator.max_value == 10

    def test_shift_by_years_initialization(self):
        """Test ShiftByYears initialization."""
        generator = ShiftByYears(1, 10)
        assert generator.min_value == 1
        assert generator.max_value == 10

    @patch("numpy.random.randint")
    def test_generate_method(self, mock_randint):
        """Test the generate method returns correct number of values."""
        mock_randint.return_value = np.array([5, 3, 7])
        generator = ShiftByHours(1, 10)

        result = generator.generate(3)

        mock_randint.assert_called_once_with(1, high=10, size=3)
        assert len(result) == 3
        assert np.array_equal(result, [5, 3, 7])

    def test_shift_method_with_matching_indices(self):
        """Test shift method with matching indices."""
        generator = ShiftByHours(1, 10)

        # Create test data with matching indices
        values = pd.Series([datetime(2023, 1, 1), datetime(2023, 1, 2)], index=[0, 1])
        shift_values = pd.Series([2, 3], index=[0, 1])

        result = generator.shift(values, shift_values)

        # Check that result has correct length and type
        assert len(result) == 2
        assert pd.api.types.is_datetime64_any_dtype(result)

        # Check that values are shifted correctly with their respective offsets
        expected = pd.Series(
            [
                datetime(2023, 1, 1) + pd.DateOffset(hours=2),
                datetime(2023, 1, 2) + pd.DateOffset(hours=3),
            ],
            index=[0, 1],
        )
        pd.testing.assert_series_equal(result, expected)

    def test_shift_method_with_mismatched_indices(self):
        """Test shift method raises error with mismatched indices."""
        generator = ShiftByHours(1, 10)

        values = pd.Series([datetime(2023, 1, 1)], index=[0])
        shift_values = pd.Series([2], index=[1])  # Different index

        with pytest.raises(
            ValueError, match="values and shift_values must have the same index"
        ):
            generator.shift(values, shift_values)

    def test_shift_by_hours_create_offset(self):
        """Test ShiftByHours._create_offset method."""
        generator = ShiftByHours(1, 10)
        offset = generator._create_offset(5)

        assert isinstance(offset, pd.DateOffset)
        assert offset.hours == 5

    def test_shift_by_days_create_offset(self):
        """Test ShiftByDays._create_offset method."""
        generator = ShiftByDays(1, 10)
        offset = generator._create_offset(3)

        assert isinstance(offset, pd.DateOffset)
        assert offset.days == 3

    def test_shift_by_weeks_create_offset(self):
        """Test ShiftByWeeks._create_offset method."""
        generator = ShiftByWeeks(1, 10)
        offset = generator._create_offset(2)

        assert isinstance(offset, pd.DateOffset)
        assert offset.weeks == 2

    def test_shift_by_months_create_offset(self):
        """Test ShiftByMonths._create_offset method."""
        generator = ShiftByMonths(1, 10)
        offset = generator._create_offset(4)

        assert isinstance(offset, pd.DateOffset)
        assert offset.months == 4

    def test_shift_by_years_create_offset(self):
        """Test ShiftByYears._create_offset method."""
        generator = ShiftByYears(1, 10)
        offset = generator._create_offset(1)

        assert isinstance(offset, pd.DateOffset)
        assert offset.years == 1


class TestTimeShiftGeneratorMap:
    """Test the time shift generator mapping functions."""

    def test_create_time_shift_gen_map(self):
        """Test _create_time_shift_gen_map returns correct mapping."""
        gen_map = _create_time_shift_gen_map()

        expected_keys = {
            "shift_by_days",
            "shift_by_hours",
            "shift_by_weeks",
            "shift_by_months",
            "shift_by_years",
            "random_days",  # Alias for shift_by_days
            "random_hours",  # Alias for shift_by_hours
        }
        assert set(gen_map.keys()) == expected_keys

        # Check that values are correct classes
        assert gen_map["shift_by_days"] == ShiftByDays
        assert gen_map["shift_by_hours"] == ShiftByHours
        assert gen_map["shift_by_weeks"] == ShiftByWeeks
        assert gen_map["shift_by_months"] == ShiftByMonths
        assert gen_map["shift_by_years"] == ShiftByYears

    def test_create_time_shift_generator_valid_methods(self):
        """Test create_time_shift_generator with valid methods."""
        methods = [
            "shift_by_days",
            "shift_by_hours",
            "shift_by_weeks",
            "shift_by_months",
            "shift_by_years",
        ]

        for method in methods:
            config = TimeShiftConfig(method=method, min=1, max=10)
            generator = create_time_shift_generator(config)

            assert isinstance(generator, TimeShiftGenerator)
            assert generator.min_value == 1
            assert generator.max_value == 10

    def test_create_time_shift_generator_invalid_method(self):
        """Test create_time_shift_generator with invalid method."""
        with pytest.raises(
            ValueError, match="Unsupported time shift method: invalid_method"
        ):
            TimeShiftConfig(method="invalid_method", min=1, max=10)


class TestDateTimeDeidentifier:
    """Test the DateTimeDeidentifier class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.idconfig = IdentifierConfig(
            name="patient_id", uid="patient_uid", description="Patient identifier"
        )

        self.time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)

        self.deid_config = DeIDConfig(time_shift=self.time_shift_config)

    def test_initialization_success(self):
        """Test successful initialization."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
            uid="test_uid",
        )

        assert transformer.idconfig == self.idconfig
        assert transformer.global_deid_config == self.deid_config
        assert transformer.datetime_column == "datetime_col"
        assert transformer.uid == "test_uid"
        assert isinstance(transformer.time_shift_generator, ShiftByDays)

    def test_initialization_without_uid(self):
        """Test initialization without uid."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
        )

        # BaseTransformer auto-generates a UID when none is provided
        assert transformer.uid is not None
        assert isinstance(transformer.time_shift_generator, ShiftByDays)

    def test_initialization_with_dependencies(self):
        """Test initialization with dependencies."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
            dependencies=["dep1", "dep2"],
        )

        assert transformer.dependencies == ["dep1", "dep2"]

    def test_initialization_none_idconfig(self):
        """Test initialization with None idconfig raises error."""
        with pytest.raises(
            ValueError, match="idconfig is required for DateTimeDeidentifier"
        ):
            DateTimeDeidentifier(
                idconfig=None,
                global_deid_config=self.deid_config,
                datetime_column="datetime_col",
            )

    def test_initialization_none_deid_config(self):
        """Test initialization with None global_deid_config raises error."""
        with pytest.raises(
            ValueError, match="global_deid_config is required for DateTimeDeidentifier"
        ):
            DateTimeDeidentifier(
                idconfig=self.idconfig,
                global_deid_config=None,
                datetime_column="datetime_col",
            )

    def test_initialization_invalid_time_shift_method(self):
        """Test initialization with invalid time shift method."""
        with pytest.raises(
            ValueError, match="Unsupported time shift method: invalid_method"
        ):
            TimeShiftConfig(method="invalid_method", min=1, max=10)

    def test_timeshift_key(self):
        """Test _timeshift_key method."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
        )

        expected_key = f"{self.idconfig.uid}_shift"
        assert transformer._timeshift_key() == expected_key

    def test_get_and_update_timeshift_mappings_empty_deid_ref_dict(self):
        """Test _get_and_update_timeshift_mappings with empty deid_ref_dict."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                ],
            }
        )

        deid_ref_dict = {}

        with patch.object(
            transformer.time_shift_generator, "generate"
        ) as mock_generate:
            mock_generate.return_value = np.array([5, 10, 15])
            result = transformer._get_and_update_timeshift_mappings(df, deid_ref_dict)

        # Check result structure
        assert len(result) == 3
        assert list(result.columns) == [self.idconfig.uid, transformer._timeshift_key()]
        assert list(result[self.idconfig.uid]) == [1, 2, 3]
        assert np.array_equal(result[transformer._timeshift_key()], [5, 10, 15])

    def test_get_and_update_timeshift_mappings_existing_mappings(self):
        """Test _get_and_update_timeshift_mappings with existing mappings."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
        )

        # Existing mappings
        existing_df = pd.DataFrame(
            {self.idconfig.uid: [1, 2], transformer._timeshift_key(): [5, 10]}
        )

        deid_ref_dict = {transformer._timeshift_key(): existing_df}

        # New data with some existing and some new values
        df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4],  # 1,2 exist; 3,4 are new
                "datetime_col": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                    datetime(2023, 1, 4),
                ],
            }
        )

        with patch.object(
            transformer.time_shift_generator, "generate"
        ) as mock_generate:
            mock_generate.return_value = np.array([20, 25])  # For new values 3,4
            result = transformer._get_and_update_timeshift_mappings(df, deid_ref_dict)

        # Check result has all 4 values
        assert len(result) == 4
        assert set(result[self.idconfig.uid]) == {1, 2, 3, 4}

        # Check existing values are preserved
        existing_rows = result[result[self.idconfig.uid].isin([1, 2])]
        assert existing_rows[transformer._timeshift_key()].tolist() == [5, 10]

    def test_get_and_update_timeshift_mappings_no_new_values(self):
        """Test _get_and_update_timeshift_mappings when no new values."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
        )

        # Existing mappings
        existing_df = pd.DataFrame(
            {self.idconfig.uid: [1, 2, 3], transformer._timeshift_key(): [5, 10, 15]}
        )

        deid_ref_dict = {transformer._timeshift_key(): existing_df}

        # New data with only existing values
        df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                ],
            }
        )

        with patch.object(
            transformer.time_shift_generator, "generate"
        ) as mock_generate:
            result = transformer._get_and_update_timeshift_mappings(df, deid_ref_dict)
            # Should not call generate since no new values
            mock_generate.assert_not_called()

        # Result should be the same as input
        pd.testing.assert_frame_equal(result, existing_df)

    def test_apply_time_shift_datetime_series(self):
        """Test _apply_time_shift with datetime series."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
        )

        datetime_series = pd.Series([datetime(2023, 1, 1), datetime(2023, 1, 2)])
        shift_series = pd.Series([5, 10])

        with patch.object(transformer.time_shift_generator, "shift") as mock_shift:
            mock_shift.return_value = pd.Series(
                [datetime(2023, 1, 6), datetime(2023, 1, 12)]
            )
            result = transformer._apply_time_shift(datetime_series, shift_series)

        mock_shift.assert_called_once_with(datetime_series, shift_series)
        assert len(result) == 2

    def test_apply_time_shift_string_series(self):
        """Test _apply_time_shift with string series (converts to datetime)."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
        )

        datetime_series = pd.Series(["2023-01-01", "2023-01-02"])
        shift_series = pd.Series([5, 10])

        with patch.object(transformer.time_shift_generator, "shift") as mock_shift:
            mock_shift.return_value = pd.Series(
                [datetime(2023, 1, 6), datetime(2023, 1, 12)]
            )

            # Call the method that uses shift
            _ = transformer._apply_time_shift(datetime_series, shift_series)

            # Should convert string to datetime before calling shift
            expected_datetime_series = pd.to_datetime(datetime_series)
            # Check that shift was called with the converted datetime series
            mock_shift.assert_called_once()
            call_args = mock_shift.call_args[0]
            pd.testing.assert_series_equal(call_args[0], expected_datetime_series)
            pd.testing.assert_series_equal(call_args[1], shift_series)

    def test_transform_success(self):
        """Test successful transform operation."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
            uid="test_transformer",
        )

        df = pd.DataFrame(
            {
                "patient_id": [1, 2],
                "datetime_col": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            }
        )

        deid_ref_dict = {}

        with (
            patch.object(
                transformer, "_get_and_update_timeshift_mappings"
            ) as mock_get_mappings,
            patch.object(transformer, "_apply_time_shift") as mock_apply_shift,
        ):
            # Mock the timeshift DataFrame
            timeshift_df = pd.DataFrame(
                {self.idconfig.uid: [1, 2], transformer._timeshift_key(): [5, 10]}
            )
            mock_get_mappings.return_value = timeshift_df

            # Mock the shifted datetime series
            shifted_series = pd.Series([datetime(2023, 1, 6), datetime(2023, 1, 12)])
            mock_apply_shift.return_value = shifted_series

            result_df, result_deid_ref_dict = transformer.transform(df, deid_ref_dict)

        # Check result DataFrame
        assert len(result_df) == 2
        assert "patient_id" in result_df.columns
        assert "datetime_col" in result_df.columns
        assert (
            transformer._timeshift_key() not in result_df.columns
        )  # Should be removed

        # Check deid_ref_dict is updated
        assert transformer._timeshift_key() in result_deid_ref_dict
        pd.testing.assert_frame_equal(
            result_deid_ref_dict[transformer._timeshift_key()], timeshift_df
        )

    def test_transform_missing_reference_column(self):
        """Test transform with missing reference column."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame(
            {
                "other_id": [1, 2],  # Wrong column name
                "datetime_col": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            }
        )

        deid_ref_dict = {}

        with pytest.raises(
            ValueError, match="Reference column 'patient_id' not found in DataFrame"
        ):
            transformer.transform(df, deid_ref_dict)

    def test_transform_merge_failure(self):
        """Test transform when merge fails (some values don't have mappings)."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                ],
            }
        )

        deid_ref_dict = {}

        with patch.object(
            transformer, "_get_and_update_timeshift_mappings"
        ) as mock_get_mappings:
            # Mock timeshift DataFrame with only 2 values (missing one)
            timeshift_df = pd.DataFrame(
                {self.idconfig.uid: [1, 2], transformer._timeshift_key(): [5, 10]}
            )
            mock_get_mappings.return_value = timeshift_df

            with pytest.raises(
                ValueError,
                match="Time shift processing failed: original length 3, processed length 2",
            ):
                transformer.transform(df, deid_ref_dict)

    def test_transform_with_existing_deid_ref_dict(self):
        """Test transform with existing deid_ref_dict."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
            uid="test_transformer",
        )

        df = pd.DataFrame(
            {
                "patient_id": [1, 2],
                "datetime_col": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            }
        )

        # Existing deid_ref_dict with other data
        existing_deid_ref_dict = {"other_key": pd.DataFrame({"col": [1, 2]})}

        with (
            patch.object(
                transformer, "_get_and_update_timeshift_mappings"
            ) as mock_get_mappings,
            patch.object(transformer, "_apply_time_shift") as mock_apply_shift,
        ):
            timeshift_df = pd.DataFrame(
                {self.idconfig.uid: [1, 2], transformer._timeshift_key(): [5, 10]}
            )
            mock_get_mappings.return_value = timeshift_df

            shifted_series = pd.Series([datetime(2023, 1, 6), datetime(2023, 1, 12)])
            mock_apply_shift.return_value = shifted_series

            _, result_deid_ref_dict = transformer.transform(df, existing_deid_ref_dict)

        # Check that existing data is preserved
        assert "other_key" in result_deid_ref_dict
        pd.testing.assert_frame_equal(
            result_deid_ref_dict["other_key"], existing_deid_ref_dict["other_key"]
        )

        # Check that new timeshift data is added
        assert transformer._timeshift_key() in result_deid_ref_dict


class TestDateTimeDeidentifierIntegration:
    """Integration tests for DateTimeDeidentifier with real data."""

    def setup_method(self):
        """Set up test fixtures."""
        self.idconfig = IdentifierConfig(
            name="patient_id", uid="patient_uid", description="Patient identifier"
        )

        self.time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)

        self.deid_config = DeIDConfig(time_shift=self.time_shift_config)

    def test_end_to_end_transform_with_real_generator(self):
        """Test end-to-end transform with real time shift generator."""
        transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
            uid="test_transformer",
        )

        # Create test data
        df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 1, 2],  # Some duplicates
                "datetime_col": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                    datetime(2023, 1, 4),  # Same patient_id as first row
                    datetime(2023, 1, 5),  # Same patient_id as second row
                ],
            }
        )

        deid_ref_dict = {}

        # Run transform
        result_df, result_deid_ref_dict = transformer.transform(df, deid_ref_dict)

        # Check that all rows are preserved
        assert len(result_df) == len(df)

        # Check that datetime values are shifted
        assert not result_df["datetime_col"].equals(df["datetime_col"])

        # Check that timeshift mappings are created
        assert transformer._timeshift_key() in result_deid_ref_dict
        timeshift_df = result_deid_ref_dict[transformer._timeshift_key()]

        # Should have 3 unique patient_ids
        assert len(timeshift_df) == 3
        assert set(timeshift_df[self.idconfig.uid]) == {1, 2, 3}

        # Check that shift values are within expected range
        shift_values = timeshift_df[transformer._timeshift_key()]
        assert all(1 <= val <= 30 for val in shift_values)

    def test_different_time_shift_methods(self):
        """Test different time shift methods work correctly."""
        methods = [
            "shift_by_hours",
            "shift_by_days",
            "shift_by_weeks",
            "shift_by_months",
            "shift_by_years",
        ]

        for method in methods:
            time_shift_config = TimeShiftConfig(method=method, min=1, max=5)
            deid_config = DeIDConfig(time_shift=time_shift_config)

            transformer = DateTimeDeidentifier(
                idconfig=self.idconfig,
                global_deid_config=deid_config,
                datetime_column="datetime_col",
                uid=f"test_{method}",
            )

            df = pd.DataFrame(
                {"patient_id": [1], "datetime_col": [datetime(2023, 1, 1)]}
            )

            result_df, _ = transformer.transform(df, {})

            # Check that transformation occurred
            assert not result_df["datetime_col"].equals(df["datetime_col"])
            assert len(result_df) == 1


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_dataframe(self):
        """Test transform with empty DataFrame."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_uid")
        time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)
        deid_config = DeIDConfig(time_shift=time_shift_config)

        transformer = DateTimeDeidentifier(
            idconfig=idconfig,
            global_deid_config=deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame(columns=["patient_id", "datetime_col"])
        deid_ref_dict = {}

        result_df, result_deid_ref_dict = transformer.transform(df, deid_ref_dict)

        assert len(result_df) == 0
        assert transformer._timeshift_key() in result_deid_ref_dict
        assert len(result_deid_ref_dict[transformer._timeshift_key()]) == 0

    def test_dataframe_with_nulls_in_reference_column(self):
        """Test transform with null values in reference column raises error."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_uid")
        time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)
        deid_config = DeIDConfig(time_shift=time_shift_config)

        transformer = DateTimeDeidentifier(
            idconfig=idconfig,
            global_deid_config=deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame(
            {
                "patient_id": [1, None, 2, None],
                "datetime_col": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                    datetime(2023, 1, 4),
                ],
            }
        )
        with pytest.raises(
            ValueError,
            match=r"Reference column 'patient_id' has null values. Time shift cannot be applied.",
        ):
            transformer.transform(df, {})

    def test_dataframe_with_nulls_in_datetime_column(self):
        """Test transform preserves null values in datetime column."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_uid")
        time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)
        deid_config = DeIDConfig(time_shift=time_shift_config)

        transformer = DateTimeDeidentifier(
            idconfig=idconfig,
            global_deid_config=deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 1),
                    None,  # Null in datetime column
                    datetime(2023, 1, 3),
                ],
            }
        )

        result_df, _ = transformer.transform(df, {})

        # Check that all rows are preserved
        assert len(result_df) == len(df)

        # Check that null in datetime column is preserved
        assert pd.isna(result_df["datetime_col"].iloc[1])

        # Check that non-null values are shifted
        assert not pd.isna(result_df["datetime_col"].iloc[0])
        assert not pd.isna(result_df["datetime_col"].iloc[2])
        assert result_df["datetime_col"].iloc[0] != df["datetime_col"].iloc[0]
        assert result_df["datetime_col"].iloc[2] != df["datetime_col"].iloc[2]

    def test_dataframe_with_all_nulls_in_datetime_column(self):
        """Test transform with all null values in datetime column."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_uid")
        time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)
        deid_config = DeIDConfig(time_shift=time_shift_config)

        transformer = DateTimeDeidentifier(
            idconfig=idconfig,
            global_deid_config=deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [None, None, None],  # All nulls
            }
        )

        result_df, _ = transformer.transform(df, {})

        # Check that all rows are preserved
        assert len(result_df) == len(df)

        # Check that all nulls are preserved
        assert result_df["datetime_col"].isna().all()

    def test_dataframe_with_mixed_nulls_in_datetime_column(self):
        """Test transform with mixed null and non-null values in datetime column."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_uid")
        time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)
        deid_config = DeIDConfig(time_shift=time_shift_config)

        transformer = DateTimeDeidentifier(
            idconfig=idconfig,
            global_deid_config=deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "datetime_col": [
                    datetime(2023, 1, 1),
                    None,
                    datetime(2023, 1, 3),
                    None,
                    datetime(2023, 1, 5),
                ],
            }
        )

        result_df, _ = transformer.transform(df, {})

        # Check that all rows are preserved
        assert len(result_df) == len(df)

        # Check that nulls are preserved at correct positions
        assert pd.isna(result_df["datetime_col"].iloc[1])
        assert pd.isna(result_df["datetime_col"].iloc[3])

        # Check that non-null values are shifted
        assert not pd.isna(result_df["datetime_col"].iloc[0])
        assert not pd.isna(result_df["datetime_col"].iloc[2])
        assert not pd.isna(result_df["datetime_col"].iloc[4])

        # Verify null positions match original
        original_null_mask = df["datetime_col"].isna()
        result_null_mask = result_df["datetime_col"].isna()
        pd.testing.assert_series_equal(original_null_mask, result_null_mask)

    def test_dataframe_with_single_null_in_reference_column(self):
        """Test transform with single null value in reference column raises error."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_uid")
        time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)
        deid_config = DeIDConfig(time_shift=time_shift_config)

        transformer = DateTimeDeidentifier(
            idconfig=idconfig,
            global_deid_config=deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame(
            {
                "patient_id": [1, None, 3],  # Single null in reference
                "datetime_col": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                ],
            }
        )

        with pytest.raises(
            ValueError,
            match=r"Reference column 'patient_id' has null values. Time shift cannot be applied.",
        ):
            transformer.transform(df, {})

    def test_dataframe_with_all_nulls_in_reference_column(self):
        """Test transform with all null values in reference column raises error."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_uid")
        time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)
        deid_config = DeIDConfig(time_shift=time_shift_config)

        transformer = DateTimeDeidentifier(
            idconfig=idconfig,
            global_deid_config=deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame(
            {
                "patient_id": [None, None, None],  # All nulls in reference
                "datetime_col": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                ],
            }
        )

        with pytest.raises(
            ValueError,
            match=r"Reference column 'patient_id' has null values. Time shift cannot be applied.",
        ):
            transformer.transform(df, {})

    def test_single_value_dataframe(self):
        """Test transform with single value DataFrame."""
        idconfig = IdentifierConfig(name="patient_id", uid="patient_uid")
        time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)
        deid_config = DeIDConfig(time_shift=time_shift_config)

        transformer = DateTimeDeidentifier(
            idconfig=idconfig,
            global_deid_config=deid_config,
            datetime_column="datetime_col",
        )

        df = pd.DataFrame({"patient_id": [1], "datetime_col": [datetime(2023, 1, 1)]})

        deid_ref_dict = {}
        result_df, result_deid_ref_dict = transformer.transform(df, deid_ref_dict)

        assert len(result_df) == 1
        assert len(result_deid_ref_dict[transformer._timeshift_key()]) == 1
        assert (
            result_deid_ref_dict[transformer._timeshift_key()][idconfig.uid].iloc[0]
            == 1
        )


class TestDateTimeDeidentifierReverse:
    """Comprehensive tests for DateTimeDeidentifier reverse functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.idconfig = IdentifierConfig(
            name="patient_id", uid="patient_uid", description="Patient identifier"
        )
        self.time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)
        self.deid_config = DeIDConfig(time_shift=self.time_shift_config)
        self.transformer = DateTimeDeidentifier(
            idconfig=self.idconfig,
            global_deid_config=self.deid_config,
            datetime_column="datetime_col",
            uid="test_transformer",
        )

    def test_reverse_success(self):
        """Test successful reverse operation restores original datetime values."""
        # Create original data
        original_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                ],
            }
        )

        # First, transform the data
        deid_ref_dict = {}
        transformed_df, deid_ref_dict = self.transformer.transform(
            original_df, deid_ref_dict
        )

        # Verify transformation occurred
        assert not transformed_df["datetime_col"].equals(original_df["datetime_col"])

        # Now reverse the transformation
        reversed_df, _ = self.transformer.reverse(transformed_df, deid_ref_dict)

        # Check that datetime values are restored (within floating point precision)
        pd.testing.assert_frame_equal(
            reversed_df[["patient_id", "datetime_col"]],
            original_df[["patient_id", "datetime_col"]],
            check_dtype=False,
        )

    def test_reverse_with_existing_timeshift_mappings(self):
        """Test reverse with pre-existing timeshift mappings in deid_ref_dict."""
        # Create shifted data (simulating already transformed data)
        shifted_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 6),  # Shifted by 5 days
                    datetime(2023, 1, 12),  # Shifted by 10 days
                    datetime(2023, 1, 18),  # Shifted by 15 days
                ],
            }
        )

        # Create timeshift mappings
        timeshift_df = pd.DataFrame(
            {
                self.idconfig.uid: [1, 2, 3],
                self.transformer._timeshift_key(): [5, 10, 15],
            }
        )
        deid_ref_dict = {self.transformer._timeshift_key(): timeshift_df}

        # Reverse the transformation
        reversed_df, _ = self.transformer.reverse(shifted_df, deid_ref_dict)

        # Check that values are restored correctly
        expected_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 1),  # 2023-01-06 - 5 days
                    datetime(2023, 1, 2),  # 2023-01-12 - 10 days
                    datetime(2023, 1, 3),  # 2023-01-18 - 15 days
                ],
            }
        )

        pd.testing.assert_frame_equal(
            reversed_df[["patient_id", "datetime_col"]],
            expected_df[["patient_id", "datetime_col"]],
            check_dtype=False,
        )

    def test_reverse_missing_timeshift_mappings_raises_error(self):
        """Test reverse raises error when timeshift mappings are missing."""
        shifted_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 6),
                    datetime(2023, 1, 12),
                    datetime(2023, 1, 18),
                ],
            }
        )

        # Empty deid_ref_dict (no timeshift mappings)
        deid_ref_dict = {}

        with pytest.raises(
            ValueError,
            match=f"Time shift reference not found for transformer {self.transformer.uid}",
        ):
            self.transformer.reverse(shifted_df, deid_ref_dict)

    def test_reverse_missing_reference_column_raises_error(self):
        """Test reverse raises error when reference column is missing."""
        shifted_df = pd.DataFrame(
            {
                "other_id": [1, 2, 3],  # Wrong column name
                "datetime_col": [
                    datetime(2023, 1, 6),
                    datetime(2023, 1, 12),
                    datetime(2023, 1, 18),
                ],
            }
        )

        timeshift_df = pd.DataFrame(
            {
                self.idconfig.uid: [1, 2, 3],
                self.transformer._timeshift_key(): [5, 10, 15],
            }
        )
        deid_ref_dict = {self.transformer._timeshift_key(): timeshift_df}

        with pytest.raises(
            ValueError, match="Reference column 'patient_id' not found in DataFrame"
        ):
            self.transformer.reverse(shifted_df, deid_ref_dict)

    def test_reverse_missing_datetime_column_raises_error(self):
        """Test reverse raises error when datetime column is missing."""
        shifted_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "other_col": [
                    datetime(2023, 1, 6),
                    datetime(2023, 1, 12),
                    datetime(2023, 1, 18),
                ],
            }
        )

        timeshift_df = pd.DataFrame(
            {
                self.idconfig.uid: [1, 2, 3],
                self.transformer._timeshift_key(): [5, 10, 15],
            }
        )
        deid_ref_dict = {self.transformer._timeshift_key(): timeshift_df}

        with pytest.raises(
            ValueError, match="Column 'datetime_col' not found in DataFrame"
        ):
            self.transformer.reverse(shifted_df, deid_ref_dict)

    def test_reverse_incomplete_mappings_raises_error(self):
        """Test reverse raises error when some values don't have shift mappings."""
        shifted_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4],  # 4 values
                "datetime_col": [
                    datetime(2023, 1, 6),
                    datetime(2023, 1, 12),
                    datetime(2023, 1, 18),
                    datetime(2023, 1, 24),
                ],
            }
        )

        # Timeshift mappings with only 3 values (missing 4)
        timeshift_df = pd.DataFrame(
            {
                self.idconfig.uid: [1, 2, 3],
                self.transformer._timeshift_key(): [5, 10, 15],
            }
        )
        deid_ref_dict = {self.transformer._timeshift_key(): timeshift_df}

        with pytest.raises(
            ValueError,
            match="Time shift reverse failed: original length 4, processed length 3",
        ):
            self.transformer.reverse(shifted_df, deid_ref_dict)

    def test_reverse_preserves_nulls_in_datetime_column(self):
        """Test reverse preserves null values in datetime column."""
        # Create shifted data with nulls
        shifted_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 6),  # Shifted
                    None,  # Null preserved
                    datetime(2023, 1, 18),  # Shifted
                ],
            }
        )

        timeshift_df = pd.DataFrame(
            {
                self.idconfig.uid: [1, 2, 3],
                self.transformer._timeshift_key(): [5, 10, 15],
            }
        )
        deid_ref_dict = {self.transformer._timeshift_key(): timeshift_df}

        reversed_df, _ = self.transformer.reverse(shifted_df, deid_ref_dict)

        # Check that null is preserved
        assert pd.isna(reversed_df["datetime_col"].iloc[1])
        # Check that non-null values are reversed
        assert not pd.isna(reversed_df["datetime_col"].iloc[0])
        assert not pd.isna(reversed_df["datetime_col"].iloc[2])

    def test_reverse_empty_dataframe(self):
        """Test reverse with empty DataFrame."""
        empty_df = pd.DataFrame(columns=["patient_id", "datetime_col"])
        timeshift_df = pd.DataFrame(
            columns=[self.idconfig.uid, self.transformer._timeshift_key()]
        )
        deid_ref_dict = {self.transformer._timeshift_key(): timeshift_df}

        reversed_df, _ = self.transformer.reverse(empty_df, deid_ref_dict)

        assert len(reversed_df) == 0
        assert list(reversed_df.columns) == ["patient_id", "datetime_col"]

    def test_reverse_different_time_shift_methods(self):
        """Test reverse works with different time shift methods."""
        methods = [
            ("shift_by_hours", pd.DateOffset(hours=5)),
            ("shift_by_days", pd.DateOffset(days=5)),
            ("shift_by_weeks", pd.DateOffset(weeks=2)),
            ("shift_by_months", pd.DateOffset(months=2)),
            ("shift_by_years", pd.DateOffset(years=1)),
        ]

        for method, _offset in methods:
            time_shift_config = TimeShiftConfig(method=method, min=1, max=10)
            deid_config = DeIDConfig(time_shift=time_shift_config)
            transformer = DateTimeDeidentifier(
                idconfig=self.idconfig,
                global_deid_config=deid_config,
                datetime_column="datetime_col",
                uid=f"test_{method}",
            )

            # Create original data
            original_df = pd.DataFrame(
                {
                    "patient_id": [1],
                    "datetime_col": [datetime(2023, 1, 1)],
                }
            )

            # Transform
            deid_ref_dict = {}
            transformed_df, deid_ref_dict = transformer.transform(
                original_df, deid_ref_dict
            )

            # Reverse
            reversed_df, _ = transformer.reverse(transformed_df, deid_ref_dict)

            # Check restoration (within reasonable precision)
            original_time = original_df["datetime_col"].iloc[0]
            reversed_time = reversed_df["datetime_col"].iloc[0]
            time_diff = abs((original_time - reversed_time).total_seconds())
            # Should be very close (within 1 second tolerance)
            assert time_diff < 1

    def test_reverse_round_trip_consistency(self):
        """Test that transform -> reverse -> transform maintains consistency."""
        original_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 1, 2],  # Duplicates
                "datetime_col": [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                    datetime(2023, 1, 4),
                    datetime(2023, 1, 5),
                ],
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
        pd.testing.assert_frame_equal(
            transformed_df[["patient_id", "datetime_col"]],
            retransformed_df[["patient_id", "datetime_col"]],
            check_dtype=False,
        )

    def test_reverse_preserves_other_columns(self):
        """Test reverse preserves other columns unchanged."""
        shifted_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 6),
                    datetime(2023, 1, 12),
                    datetime(2023, 1, 18),
                ],
                "other_col": ["A", "B", "C"],
                "numeric_col": [10, 20, 30],
            }
        )

        timeshift_df = pd.DataFrame(
            {
                self.idconfig.uid: [1, 2, 3],
                self.transformer._timeshift_key(): [5, 10, 15],
            }
        )
        deid_ref_dict = {self.transformer._timeshift_key(): timeshift_df}

        reversed_df, _ = self.transformer.reverse(shifted_df, deid_ref_dict)

        # Check other columns are preserved
        pd.testing.assert_series_equal(
            reversed_df["other_col"], shifted_df["other_col"]
        )
        pd.testing.assert_series_equal(
            reversed_df["numeric_col"], shifted_df["numeric_col"]
        )

    def test_reverse_with_string_datetime_values(self):
        """Test reverse works with string datetime values."""
        shifted_df = pd.DataFrame(
            {
                "patient_id": [1, 2],
                "datetime_col": ["2023-01-06", "2023-01-12"],  # String format
            }
        )

        timeshift_df = pd.DataFrame(
            {
                self.idconfig.uid: [1, 2],
                self.transformer._timeshift_key(): [5, 10],
            }
        )
        deid_ref_dict = {self.transformer._timeshift_key(): timeshift_df}

        reversed_df, _ = self.transformer.reverse(shifted_df, deid_ref_dict)

        # Check that values are reversed (converted to datetime and shifted back)
        assert len(reversed_df) == 2
        assert pd.api.types.is_datetime64_any_dtype(reversed_df["datetime_col"])

    def test_reverse_with_missing_uid_column_in_timeshift_df(self):
        """Test reverse raises error when UID column is missing in timeshift_df."""
        shifted_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 6),
                    datetime(2023, 1, 12),
                    datetime(2023, 1, 18),
                ],
            }
        )

        # Timeshift_df missing UID column
        timeshift_df = pd.DataFrame(
            {
                self.transformer._timeshift_key(): [5, 10, 15],  # Missing uid column
            }
        )
        deid_ref_dict = {self.transformer._timeshift_key(): timeshift_df}

        with pytest.raises(
            ValueError,
            match=f"UID column '{self.idconfig.uid}' not found in timeshift_df",
        ):
            self.transformer.reverse(shifted_df, deid_ref_dict)

    def test_reverse_with_missing_shift_column_in_timeshift_df(self):
        """Test reverse raises error when shift column is missing in timeshift_df."""
        shifted_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "datetime_col": [
                    datetime(2023, 1, 6),
                    datetime(2023, 1, 12),
                    datetime(2023, 1, 18),
                ],
            }
        )

        # Timeshift_df missing shift column
        timeshift_df = pd.DataFrame(
            {
                self.idconfig.uid: [1, 2, 3],  # Missing shift column
            }
        )
        deid_ref_dict = {self.transformer._timeshift_key(): timeshift_df}

        with pytest.raises(
            ValueError,
            match=f"Shift column '{self.transformer._timeshift_key()}' not found in timeshift_df",
        ):
            self.transformer.reverse(shifted_df, deid_ref_dict)
