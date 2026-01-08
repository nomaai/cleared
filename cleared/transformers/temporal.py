"""
Transformers for temporal data de-identification.

This module provides transformers for de-identifying temporal data,
including date and time shifting operations while preserving relative relationships.
"""

from __future__ import annotations

import pandas as pd
import numpy as np
import logging
from abc import ABC, abstractmethod
from cleared.transformers.base import FilterableTransformer
from cleared.config.structure import (
    IdentifierConfig,
    DeIDConfig,
    TimeShiftConfig,
    FilterConfig,
)
from cleared.models.verify_models import ColumnComparisonResult

# Set up logger for this module
logger = logging.getLogger(__name__)

# Set up logger for this module
logger = logging.getLogger(__name__)


class DateTimeDeidentifier(FilterableTransformer):
    """
    De-identifier for date and time columns using time shifting.

    This transformer applies time shifts to datetime columns based on reference column values
    (e.g., patient_id). The same reference value will always receive the same time shift,
    ensuring consistency across multiple datetime columns for the same entity.
    """

    def __init__(
        self,
        idconfig: IdentifierConfig | dict,
        datetime_column: str,
        filter_config: FilterConfig | None = None,
        value_cast: str | None = None,
        uid: str | None = None,
        dependencies: list[str] | None = None,
        global_deid_config: DeIDConfig | None = None,
    ):
        """
        Initialize the DateTimeDeidentifier.

        Args:
            idconfig: Configuration for the reference column used for time shifting
            datetime_column: Name of the datetime column to shift
            filter_config: Configuration for filtering operations
            value_cast: Type to cast the de-identification column to
            uid: Unique identifier for the transformer
            dependencies: List of dependency UIDs
            global_deid_config: Global de-identification configuration (preferred)

        Raises:
            ValueError: If idconfig is None
            ValueError: If global_deid_config is None or missing time_shift configuration
            ValueError: If time_shift method is not supported

        """
        super().__init__(
            filter_config=filter_config,
            value_cast=value_cast,
            uid=uid,
            dependencies=dependencies,
            global_deid_config=global_deid_config,
        )

        # Handle both IdentifierConfig object and dict
        if isinstance(idconfig, dict):
            if "idconfig" in idconfig:
                self.idconfig = IdentifierConfig(**idconfig["idconfig"])
            else:
                self.idconfig = IdentifierConfig(**idconfig)
        else:
            self.idconfig = idconfig

        self.datetime_column = datetime_column

        if self.idconfig is None:
            logger.error(f"Transformer {self.uid} idconfig is None")
            raise ValueError("idconfig is required for DateTimeDeidentifier")

        # Use global_deid_config from parent (set by engine)
        # deid_config parameter is kept for backward compatibility but ignored
        if self.global_deid_config is None:
            error_msg = "global_deid_config is required for DateTimeDeidentifier. Ensure the engine passes global_deid_config when instantiating transformers."
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

        if self.global_deid_config.time_shift is None:
            error_msg = "time_shift configuration is required in global deid_config for DateTimeDeidentifier"
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

        # Validate that the time shift method is supported
        if (
            self.global_deid_config.time_shift.method
            not in _create_time_shift_gen_map().keys()
        ):
            error_msg = f"Unsupported time shift method: {self.global_deid_config.time_shift.method}"
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

        self.time_shift_generator = create_time_shift_generator(
            self.global_deid_config.time_shift
        )

    def _apply_transform(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Transform date/time data by applying time shifting based on reference column.

        This method:
        1. Checks if deid_ref_dict has a timeshift entry
        2. Creates new entries for missing reference values with random shift amounts
        3. Applies time shift method to de-identify date/time values
        4. Validates that all rows were successfully processed

        Args:
            df: DataFrame containing the data to transform
            deid_ref_dict: Dictionary of deidentification reference DataFrames, keyed by transformer UID

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        Raises:
            ValueError: If required columns are not found or processing fails
            ValueError: If the reference column (idconfig.name) contains any null values

        Null Handling:
            - Reference column (idconfig.name): Null values are NOT allowed. If any null values
              are present in the reference column, a ValueError is raised. This is because
              time shifts are applied per reference value, and null values cannot be used as
              keys for shift mappings.
            - Datetime column (datetime_column): Null values ARE preserved. If the datetime
              column contains null values (NaT), they will remain as null in the output.
              The time shift operation preserves nulls when applying date offsets.

        """
        return self._apply_datetime_deid(df, deid_ref_dict, reverse=False)

    def _apply_reverse(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Reverse the time shift transformation to restore original datetime values.

        This method:
        1. Gets the timeshift_df from deid_ref_dict
        2. Joins df with timeshift_df to get shift amounts
        3. Applies negative shift to restore original datetime values

        Args:
            df: DataFrame containing the shifted datetime data to reverse
            deid_ref_dict: Dictionary of deidentification reference DataFrames, keyed by transformer UID

        Returns:
            Tuple of (reversed_df, updated_deid_ref_dict)

        Raises:
            ValueError: If required columns are not found
            ValueError: If timeshift_df is not found in deid_ref_dict
            ValueError: If some values don't have shift mappings

        """
        return self._apply_datetime_deid(df, deid_ref_dict, reverse=True)

    def _apply_datetime_deid(
        self,
        df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
        reverse: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Apply or reverse datetime de-identification using time shifting.

        This is a helper function that handles both forward transformation and reverse
        transformation. The `reverse` parameter determines the direction of the operation.

        Args:
            df: DataFrame containing the data to transform or reverse
            deid_ref_dict: Dictionary of deidentification reference DataFrames, keyed by transformer UID
            reverse: If True, reverse the transformation (restore original values).
                    If False, apply transformation (shift datetime values).

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        Raises:
            ValueError: If required columns are not found
            ValueError: If timeshift_df is not found in deid_ref_dict (when reverse=True)
            ValueError: If the reference column contains null values (when reverse=False)
            ValueError: If some values don't have shift mappings

        """
        deid_ref_dict = deid_ref_dict.copy()
        # Validate input
        self._validate_datetime_deid_args(df)

        # Validate datetime column format
        self._validate_datetime_column_format(df)

        # Handle empty DataFrame
        if len(df) == 0:
            if reverse:
                return df.copy(), deid_ref_dict.copy()
            else:
                deid_ref_dict[self._timeshift_key()] = (
                    self._get_and_update_timeshift_mappings(df, deid_ref_dict)
                )
                return df.copy(), deid_ref_dict.copy()

        # Perform the timeshift operation.
        timeshift_df = self._get_timeshift_reference(df, deid_ref_dict, reverse)
        merged = self._merge_with_timeshift(df, timeshift_df)
        self._validate_merge_success(df, merged, reverse)

        # Prepare the outputs.
        merged = self._apply_timeshift_to_column(merged, reverse)
        merged = self._remove_shift_columns(merged)
        updated_deid_ref_dict = deid_ref_dict.copy()
        if not reverse:
            updated_deid_ref_dict[self._timeshift_key()] = timeshift_df.copy()

        return merged, updated_deid_ref_dict

    def _get_column_to_cast(self) -> str | None:
        """Get the column name to cast (the datetime column being de-identified)."""
        return self.datetime_column

    def _validate_datetime_deid_args(self, df: pd.DataFrame) -> None:
        """
        Validate that required columns exist in the DataFrame.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If reference column or datetime column is not found

        """
        if self.idconfig.name not in df.columns:
            error_msg = (
                f"Reference column '{self.idconfig.name}' not found in DataFrame"
            )
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)
        if self.datetime_column not in df.columns:
            error_msg = f"Column '{self.datetime_column}' not found in DataFrame"
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

    def _validate_datetime_column_format(self, df: pd.DataFrame) -> None:
        """
        Validate that the datetime column has consistent format that can be parsed.

        This validation checks if all values in the datetime column can be successfully
        parsed as datetime values. It identifies rows with inconsistent formats and provides
        helpful error messages with examples.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If datetime column has inconsistent formats or unparseable values

        """
        datetime_col = df[self.datetime_column]

        # If already datetime type, no need to validate format
        if pd.api.types.is_datetime64_any_dtype(datetime_col):
            return

        # If not string/object type, skip format validation
        if not pd.api.types.is_string_dtype(
            datetime_col
        ) and not pd.api.types.is_object_dtype(datetime_col):
            return

        # Skip validation for empty column
        if len(datetime_col) == 0:
            return

        # Try to parse with pandas to_datetime using 'coerce' to identify failures
        # Using 'mixed' format allows pandas to infer format for each element individually
        # This will convert unparseable values to NaT
        try:
            parsed = pd.to_datetime(datetime_col, errors="coerce", format="mixed")
        except Exception as e:
            # If parsing fails entirely, provide a helpful error
            error_msg = (
                f"Failed to parse datetime column '{self.datetime_column}': {e!s}\n"
                f"Please ensure all values in the datetime column have a consistent format, "
                f"or use 'value_cast: datetime' in your transformer configuration."
            )
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg) from e

        # Find rows that failed to parse (became NaT) but were not originally null
        original_is_null = datetime_col.isna()
        parsed_is_null = parsed.isna()
        failed_to_parse = ~original_is_null & parsed_is_null

        if failed_to_parse.any():
            failed_count = int(failed_to_parse.sum())
            failed_indices = datetime_col[failed_to_parse].index.tolist()

            # Get up to 5 example values that failed
            example_values = datetime_col.loc[failed_indices[:5]].tolist()

            # Build error message
            error_msg = (
                f"Datetime column '{self.datetime_column}' contains values with inconsistent formats. "
                f"{failed_count} row(s) could not be parsed as datetime values."
            )

            if example_values:
                example_str = ", ".join([f'"{val}"' for val in example_values])
                error_msg += f"\nExample values that failed to parse: {example_str}"

            error_msg += (
                "\n\nTo fix this, ensure all values in the datetime column have a consistent format, "
                "or use 'value_cast: datetime' in your transformer configuration to allow pandas "
                "to infer formats automatically."
            )

            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

    def _get_timeshift_reference(
        self,
        df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
        reverse: bool,
    ) -> pd.DataFrame:
        """
        Get or create timeshift reference DataFrame based on mode.

        Args:
            df: DataFrame containing the data to transform or reverse
            deid_ref_dict: Dictionary of deidentification reference DataFrames
            reverse: If True, get existing timeshift_df (must exist). If False, create/update mappings.

        Returns:
            DataFrame with reference values and their shift amounts

        Raises:
            ValueError: If timeshift_df is not found in deid_ref_dict (when reverse=True)
            ValueError: If required columns are missing in timeshift_df (when reverse=True)
            ValueError: If reference column has null values (when reverse=False)

        """
        if reverse:
            # In reverse mode, timeshift_df must already exist
            timeshift_df = deid_ref_dict.get(self._timeshift_key())
            if timeshift_df is None:
                error_msg = f"Time shift reference not found for transformer {self.uid or 'unnamed'} and identifier {self.idconfig.name}"
                logger.error(f"Transformer {self.uid} {error_msg}")
                raise ValueError(error_msg)

            if self.idconfig.uid not in timeshift_df.columns:
                error_msg = f"UID column '{self.idconfig.uid}' not found in timeshift_df for transformer {self.uid or 'unnamed'}"
                logger.error(f"Transformer {self.uid} {error_msg}")
                raise ValueError(error_msg)

            if self._timeshift_key() not in timeshift_df.columns:
                error_msg = f"Shift column '{self._timeshift_key()}' not found in timeshift_df for transformer {self.uid or 'unnamed'}"
                logger.error(f"Transformer {self.uid} {error_msg}")
                raise ValueError(error_msg)
        else:
            # In forward mode, create/update timeshift mappings
            timeshift_df = self._get_and_update_timeshift_mappings(df, deid_ref_dict)

            # Validate that reference column has no nulls (only in forward mode)
            if df[self.idconfig.name].isna().any():
                error_msg = f"Reference column '{self.idconfig.name}' has null values. Time shift cannot be applied. Please ensure all reference values are non-null."
                logger.error(f"Transformer {self.uid} {error_msg}")
                raise ValueError(error_msg)

        return timeshift_df

    def _merge_with_timeshift(
        self, df: pd.DataFrame, timeshift_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge input DataFrame with timeshift reference DataFrame.

        Args:
            df: Input DataFrame to merge
            timeshift_df: Timeshift reference DataFrame

        Returns:
            Merged DataFrame

        """
        return df.merge(
            timeshift_df[[self.idconfig.uid, self._timeshift_key()]],
            left_on=self.idconfig.name,
            right_on=self.idconfig.uid,
            how="inner",
        )

    def _validate_merge_success(
        self, original_df: pd.DataFrame, merged_df: pd.DataFrame, reverse: bool
    ) -> None:
        """
        Validate that the merge operation was successful.

        Args:
            original_df: Original DataFrame before merge
            merged_df: DataFrame after merge
            reverse: Whether this is a reverse operation

        Raises:
            ValueError: If merge was not successful (rows were lost)

        """
        if merged_df.shape[0] != original_df.shape[0]:
            operation = "reverse" if reverse else "processing"
            error_msg = f"Time shift {operation} failed: original length {original_df.shape[0]}, processed length {merged_df.shape[0]}. Some reference values don't have shift mappings."
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

    def _apply_timeshift_to_column(
        self, merged_df: pd.DataFrame, reverse: bool
    ) -> pd.DataFrame:
        """
        Apply time shift to the datetime column (forward or reverse).

        Args:
            merged_df: DataFrame with merged timeshift data
            reverse: If True, apply reverse shift. If False, apply forward shift.

        Returns:
            DataFrame with shifted datetime column

        """
        if reverse:
            merged_df[self.datetime_column] = self._apply_reverse_time_shift(
                merged_df[self.datetime_column], merged_df[self._timeshift_key()]
            )
        else:
            merged_df[self.datetime_column] = self._apply_time_shift(
                merged_df[self.datetime_column], merged_df[self._timeshift_key()]
            )
        return merged_df

    def _remove_shift_columns(self, merged_df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove unnecessary shift columns and reference columns from merged DataFrame.

        Args:
            merged_df: DataFrame with shift columns to remove

        Returns:
            DataFrame with shift columns removed

        """
        columns_to_drop = [self._timeshift_key()]
        if self.idconfig.uid != self.idconfig.name:
            columns_to_drop.append(self.idconfig.uid)
        if columns_to_drop:
            merged_df = merged_df.drop(columns=columns_to_drop)
        return merged_df

    def _get_and_update_timeshift_mappings(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Get and update timeshift mappings for the reference column.

        Args:
            df: DataFrame containing the data to transform
            deid_ref_dict: Dictionary of deidentification reference DataFrames

        Returns:
            DataFrame with reference values and their shift amounts

        """
        # Get existing timeshift DataFrame or create empty one
        timeshift_df = deid_ref_dict.get(
            self._timeshift_key(),
            pd.DataFrame({self.idconfig.uid: [], self._timeshift_key(): []}),
        )

        # Get unique values from the reference column
        unique_values = df[self.idconfig.name].dropna().unique()

        # Find values that don't have shift mappings
        existing_values = set(timeshift_df[self.idconfig.uid].dropna().unique())
        missing_values = set(unique_values) - existing_values

        if missing_values:
            # Generate new shift amounts for missing values
            new_shifts = self.time_shift_generator.generate(len(missing_values))
            new_mappings = pd.DataFrame(
                {
                    self.idconfig.uid: list(missing_values),
                    self._timeshift_key(): new_shifts,
                }
            )
            timeshift_df = pd.concat([timeshift_df, new_mappings], ignore_index=True)
            logger.debug(
                f"Transformer {self.uid} generated {len(missing_values)} new time shift mappings for column '{self.datetime_column}'"
            )

        return timeshift_df

    def _apply_time_shift(
        self, datetime_series: pd.Series, shift_series: pd.Series
    ) -> pd.Series:
        """
        Apply time shift to datetime values.

        Args:
            datetime_series: Series of datetime values to shift
            shift_series: Series of shift amounts

        Returns:
            Series of shifted datetime values

        """
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(datetime_series):
            datetime_series = pd.to_datetime(datetime_series)

        return self.time_shift_generator.shift(datetime_series, shift_series)

    def _timeshift_key(self) -> str:
        return f"{self.idconfig.uid}_shift"

    def _get_column_to_cast(self) -> str | None:
        """Get the column name to cast (the datetime column being de-identified)."""
        return self.datetime_column

    def _compare(
        self,
        original_df: pd.DataFrame,
        reversed_df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
    ):
        """
        Compare original and reversed datetime columns to verify correctness.

        Args:
            original_df: Filtered and cast original DataFrame
            reversed_df: Filtered and cast reversed DataFrame
            deid_ref_dict: Dictionary of de-identification reference DataFrames

        Returns:
            List of ColumnComparisonResult objects

        """
        column_name = self.datetime_column

        if column_name not in original_df.columns:
            return [
                ColumnComparisonResult(
                    column_name=column_name,
                    status="error",
                    message=f"Column '{column_name}' not found in original DataFrame",
                    original_length=len(original_df),
                    reversed_length=len(reversed_df),
                    mismatch_count=0,
                    mismatch_percentage=0.0,
                )
            ]

        if column_name not in reversed_df.columns:
            return [
                ColumnComparisonResult(
                    column_name=column_name,
                    status="error",
                    message=f"Column '{column_name}' not found in reversed DataFrame",
                    original_length=len(original_df),
                    reversed_length=len(reversed_df),
                    mismatch_count=0,
                    mismatch_percentage=0.0,
                )
            ]

        original_series = original_df[column_name].reset_index(drop=True)
        reversed_series = reversed_df[column_name].reset_index(drop=True)

        original_length = len(original_series)
        reversed_length = len(reversed_series)

        # Check length match
        if original_length != reversed_length:
            return [
                ColumnComparisonResult(
                    column_name=column_name,
                    status="error",
                    message=f"Column '{column_name}' length mismatch: original has {original_length} rows, reversed has {reversed_length} rows",
                    original_length=original_length,
                    reversed_length=reversed_length,
                    mismatch_count=abs(original_length - reversed_length),
                    mismatch_percentage=100.0,
                )
            ]

        # Convert to datetime if not already (handles string datetime formats)
        if not pd.api.types.is_datetime64_any_dtype(original_series):
            original_series = pd.to_datetime(original_series, errors="coerce")
        if not pd.api.types.is_datetime64_any_dtype(reversed_series):
            reversed_series = pd.to_datetime(reversed_series, errors="coerce")

        # Compare values - handle NaN properly
        both_nan = original_series.isna() & reversed_series.isna()
        both_not_nan = ~original_series.isna() & ~reversed_series.isna()
        values_equal = (original_series == reversed_series) & both_not_nan

        # Mismatches are rows where values are not equal AND not both NaN
        mismatches = ~(both_nan | values_equal)
        mismatch_count = int(mismatches.sum())

        if mismatch_count == 0:
            return [
                ColumnComparisonResult(
                    column_name=column_name,
                    status="pass",
                    message=f"Column '{column_name}' matches perfectly",
                    original_length=original_length,
                    reversed_length=reversed_length,
                    mismatch_count=0,
                    mismatch_percentage=0.0,
                )
            ]

        mismatch_percentage = (mismatch_count / original_length) * 100.0
        sample_mismatch_indices = original_series[mismatches].index.tolist()[:100]

        return [
            ColumnComparisonResult(
                column_name=column_name,
                status="error",
                message=f"Column '{column_name}' has {mismatch_count} mismatches ({mismatch_percentage:.2f}%)",
                original_length=original_length,
                reversed_length=reversed_length,
                mismatch_count=mismatch_count,
                mismatch_percentage=mismatch_percentage,
                sample_mismatch_indices=sample_mismatch_indices,
            )
        ]

    def _apply_reverse_time_shift(
        self, datetime_series: pd.Series, shift_series: pd.Series
    ) -> pd.Series:
        """
        Apply reverse time shift to datetime values (subtract shift instead of add).

        Args:
            datetime_series: Series of shifted datetime values to reverse
            shift_series: Series of shift amounts to subtract

        Returns:
            Series of original datetime values

        """
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(datetime_series):
            datetime_series = pd.to_datetime(datetime_series)

        # Apply negative shift by subtracting instead of adding
        return datetime_series.combine(
            shift_series,
            lambda v, m: v - self.time_shift_generator._create_offset(int(m)),
        )


class TimeShiftGenerator(ABC):
    """Abstract class for generating time shift values in hours."""

    def __init__(self, min_value: int, max_value: int):
        """
        Initialize the time shift generator.

        Args:
            min_value: Minimum shift value
            max_value: Maximum shift value

        """
        self.min_value = min_value
        self.max_value = max_value

    def generate(self, count: int) -> float:
        """Generate random shift values."""
        return np.random.randint(self.min_value, high=self.max_value, size=count)

    def shift(self, values: pd.Series, shift_values: pd.Series) -> pd.Series:
        """Apply time shifts to datetime values."""
        if not values.index.equals(shift_values.index):
            raise ValueError("values and shift_values must have the same index")

        return values.combine(
            shift_values, lambda v, m: v + self._create_offset(int(m))
        )

    @abstractmethod
    def _create_offset(self, value: int) -> pd.DateOffset:
        raise NotImplementedError("Subclasses must implement this method")


class ShiftByHours(TimeShiftGenerator):
    """Time shift generator that shifts by hours."""

    def __init__(self, min_value: int, max_value: int):
        """Initialize shift by hours generator."""
        super().__init__(min_value, max_value)

    def _create_offset(self, value: int) -> pd.DateOffset:
        return pd.DateOffset(hours=value)


class ShiftByDays(TimeShiftGenerator):
    """Time shift generator that shifts by days."""

    def __init__(self, min_value: int, max_value: int):
        """Initialize shift by days generator."""
        super().__init__(min_value, max_value)

    def _create_offset(self, value: int) -> pd.DateOffset:
        return pd.DateOffset(days=value)


class ShiftByWeeks(TimeShiftGenerator):
    """Time shift generator that shifts by weeks."""

    def __init__(self, min_value: int, max_value: int):
        """Initialize shift by weeks generator."""
        super().__init__(min_value, max_value)

    def _create_offset(self, value: int) -> pd.DateOffset:
        return pd.DateOffset(weeks=value)


class ShiftByMonths(TimeShiftGenerator):
    """Time shift generator that shifts by months."""

    def __init__(self, min_value: int, max_value: int):
        """Initialize shift by months generator."""
        super().__init__(min_value, max_value)

    def _create_offset(self, value: int) -> pd.DateOffset:
        return pd.DateOffset(months=value)


class ShiftByYears(TimeShiftGenerator):
    """Time shift generator that shifts by years."""

    def __init__(self, min_value: int, max_value: int):
        """Initialize shift by years generator."""
        super().__init__(min_value, max_value)

    def _create_offset(self, value: int) -> pd.DateOffset:
        return pd.DateOffset(years=value)


def _create_time_shift_gen_map() -> dict:
    generator_map = {
        "shift_by_days": ShiftByDays,
        "shift_by_hours": ShiftByHours,
        "shift_by_weeks": ShiftByWeeks,
        "shift_by_months": ShiftByMonths,
        "shift_by_years": ShiftByYears,
        "random_days": ShiftByDays,  # Alias for shift_by_days
        "random_hours": ShiftByHours,  # Alias for shift_by_hours
    }
    return generator_map


def create_time_shift_generator(config: TimeShiftConfig) -> TimeShiftGenerator:
    """
    Create and return the appropriate time shift generator.

    Args:
        config: TimeShiftConfig object

    Returns:
        TimeShiftGenerator object

    """
    generator_map = _create_time_shift_gen_map()
    if config.method not in generator_map:
        error_msg = f"Unsupported time shift method: {config.method}"
        logger.error(f"Time shift generator {error_msg}")
        raise ValueError(error_msg)

    return generator_map[config.method](config.min, config.max)
