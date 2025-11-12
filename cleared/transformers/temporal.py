"""
Transformers for temporal data de-identification.

This module provides transformers for de-identifying temporal data,
including date and time shifting operations while preserving relative relationships.
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from cleared.transformers.base import FilterableTransformer
from cleared.config.structure import (
    IdentifierConfig,
    DeIDConfig,
    TimeShiftConfig,
    FilterConfig,
)


class DateTimeDeidentifier(FilterableTransformer):
    """
    De-identifier for date and time columns using time shifting.

    This transformer applies time shifts to datetime columns based on reference column values
    (e.g., patient_id). The same reference value will always receive the same time shift,
    ensuring consistency across multiple datetime columns for the same entity.

    Null Handling:
        - Reference column (idconfig.name): Null values are NOT allowed. The transformer
          will raise a ValueError if any null values are detected in the reference column.
          This is required because time shifts are keyed by reference values.
        - Datetime column (datetime_column): Null values ARE preserved. If the datetime
          column contains null/NaT values, they will remain as null in the output after
          transformation. The time shift operation does not modify null datetime values.

    Examples:
        >>> from cleared.config.structure import IdentifierConfig, DeIDConfig, TimeShiftConfig
        >>> idconfig = IdentifierConfig(name="patient_id", uid="patient_uid")
        >>> deid_config = DeIDConfig(time_shift=TimeShiftConfig(method="shift_by_days", min=-30, max=30))
        >>> transformer = DateTimeDeidentifier(
        ...     idconfig=idconfig,
        ...     global_deid_config=deid_config,
        ...     datetime_column="admission_date"
        ... )
        >>> # Transform data - nulls in datetime column are preserved
        >>> result_df, deid_ref = transformer.transform(df, {})

    """

    def __init__(
        self,
        idconfig: IdentifierConfig,
        datetime_column: str,
        deid_config: DeIDConfig | None = None,
        time_shift_method: str | None = None,
        filter_config: FilterConfig | None = None,
        value_cast: str | None = None,
        uid: str | None = None,
        dependencies: list[str] | None = None,
        global_deid_config: DeIDConfig | None = None,
    ):
        """
        Initialize DateTimeDeidentifier.

        Args:
            idconfig: Configuration for the reference column used for time shifting
            datetime_column: Name of the datetime column to shift
            deid_config: De-identification configuration (deprecated, use global_deid_config)
            time_shift_method: Name of the time shift method to apply (deprecated)
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
        self.idconfig = idconfig
        self.datetime_column = datetime_column

        if self.idconfig is None:
            raise ValueError("idconfig is required for DateTimeDeidentifier")

        # Use global_deid_config from parent (set by engine)
        # deid_config parameter is kept for backward compatibility but ignored
        if self.global_deid_config is None:
            raise ValueError(
                "global_deid_config is required for DateTimeDeidentifier. "
                "Ensure the engine passes global_deid_config when instantiating transformers."
            )

        if self.global_deid_config.time_shift is None:
            raise ValueError(
                "time_shift configuration is required in global deid_config for DateTimeDeidentifier"
            )

        # Validate that the time shift method is supported
        if (
            self.global_deid_config.time_shift.method
            not in _create_time_shift_gen_map().keys()
        ):
            raise ValueError(
                f"Unsupported time shift method: {self.global_deid_config.time_shift.method}"
            )

        self.time_shift_generator = create_time_shift_generator(
            self.global_deid_config.time_shift
        )

    def _get_column_to_cast(self) -> str | None:
        """Get the column name to cast (the datetime column being de-identified)."""
        return self.datetime_column

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

        Examples:
            >>> # Valid: No nulls in reference column, nulls in datetime are preserved
            >>> df = pd.DataFrame({
            ...     "patient_id": [1, 2, 3],
            ...     "admission_date": [datetime(2023, 1, 1), None, datetime(2023, 1, 3)]
            ... })
            >>> result_df, _ = transformer.transform(df, {})
            >>> # Result: admission_date will have [shifted_date, NaT, shifted_date]

            >>> # Invalid: Nulls in reference column will raise ValueError
            >>> df = pd.DataFrame({
            ...     "patient_id": [1, None, 3],
            ...     "admission_date": [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)]
            ... })
            >>> transformer.transform(df, {})  # Raises ValueError

        """
        deid_ref_dict = deid_ref_dict.copy()
        # Validate input
        if self.idconfig.name not in df.columns:
            raise ValueError(
                f"Reference column '{self.idconfig.name}' not found in DataFrame"
            )
        if self.datetime_column not in df.columns:
            raise ValueError(f"Column '{self.datetime_column}' not found in DataFrame")

        # Handle empty DataFrame
        if len(df) == 0:
            deid_ref_dict[self._timeshift_key()] = (
                self._get_and_update_timeshift_mappings(df, deid_ref_dict)
            )
            return df.copy(), deid_ref_dict.copy()

        # Get or create timeshift reference DataFrame
        timeshift_df = self._get_and_update_timeshift_mappings(df, deid_ref_dict)

        if df[self.idconfig.name].isna().any():
            raise ValueError(
                f"Reference column '{self.idconfig.name}' has null values. Time shift cannot be applied. Please ensure all reference values are non-null."
            )

        # Merge input DataFrame with timeshift reference DataFrame
        merged = df.merge(
            timeshift_df[[self.idconfig.uid, self._timeshift_key()]],
            left_on=self.idconfig.name,
            right_on=self.idconfig.uid,
            how="inner",
        )

        # Check if merge was successful (all non-null rows preserved)
        if merged.shape[0] != df.shape[0]:
            raise ValueError(
                f"Time shift processing failed: original length {df.shape[0]}, "
                f"processed length {merged.shape[0]}. Some reference values don't have shift mappings."
            )

        # Apply time shift to the datetime column
        merged[self.datetime_column] = self._apply_time_shift(
            merged[self.datetime_column], merged[self._timeshift_key()]
        )

        # Remove the shift column and reference column
        merged.drop(columns=[self.idconfig.uid, self._timeshift_key()], inplace=True)

        # Update the deid_ref_dict with the timeshift DataFrame
        updated_deid_ref_dict = deid_ref_dict.copy()
        updated_deid_ref_dict[self._timeshift_key()] = timeshift_df.copy()

        return merged, updated_deid_ref_dict

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
        raise ValueError(f"Unsupported time shift method: {config.method}")

    return generator_map[config.method](config.min, config.max)
