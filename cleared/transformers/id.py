"""
Transformers for ID de-identification.

This module provides transformers for de-identifying ID columns in DataFrames,
replacing original identifiers with sequential integer values while maintaining
referential integrity across multiple tables.
"""

from __future__ import annotations

import pandas as pd
import numpy as np
import logging
from cleared.transformers.base import FilterableTransformer
from cleared.config.structure import IdentifierConfig, FilterConfig, DeIDConfig
from cleared.models.verify_models import ColumnComparisonResult

# Set up logger for this module
logger = logging.getLogger(__name__)


class IDDeidentifier(FilterableTransformer):
    """
    De-identifier for ID columns.

    This transformer replaces original ID values with sequential integer de-identified
    values. It maintains a mapping dictionary that can be shared across multiple tables
    to ensure consistent de-identification of the same ID values.

    The transformer supports:
    - Filtering rows before de-identification
    - Type casting to ensure consistent types (e.g., string IDs to integers)
    - Accumulating mappings across multiple segments/batches
    - Reverse transformation to restore original values

    Attributes:
        idconfig: IdentifierConfig specifying the column to de-identify
        filter_config: Optional filter configuration for row filtering
        value_cast: Optional type casting specification

    """

    def __init__(
        self,
        idconfig: IdentifierConfig | dict,
        filter_config: FilterConfig | None = None,
        value_cast: str | None = None,
        uid: str | None = None,
        dependencies: list[str] | None = None,
        global_deid_config: DeIDConfig | None = None,
    ):
        """
        Initialize the IDDeidentifier transformer.

        Args:
            idconfig: Configuration for the ID column to de-identify.
                     Can be an IdentifierConfig object or a dict that will be converted.
            filter_config: Optional filter configuration for row filtering before transformation.
            value_cast: Optional type to cast the de-identification column to.
                       Supported values: "integer", "float", "string", "datetime".
            uid: Optional unique identifier for the transformer. If not provided, a UUID is generated.
            dependencies: Optional list of dependency UIDs for transformer ordering.
            global_deid_config: Optional global de-identification configuration.

        Raises:
            ValueError: If idconfig is None

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
            # If the dict has an 'idconfig' key, extract it
            if "idconfig" in idconfig:
                self.idconfig = IdentifierConfig(**idconfig["idconfig"])
            else:
                self.idconfig = IdentifierConfig(**idconfig)
        else:
            self.idconfig = idconfig

        if self.idconfig is None:
            logger.error(f"Transformer {self.uid} idconfig is None")
            raise ValueError("idconfig is required for IDDeidentifier")

    def _get_column_to_cast(self) -> str | None:
        """
        Get the column name to cast (the ID column being de-identified).

        Returns:
            Column name to cast, or None if idconfig is not set

        """
        return self.idconfig.name if self.idconfig else None

    def _compare(
        self,
        original_df: pd.DataFrame,
        reversed_df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
    ) -> list[ColumnComparisonResult]:
        """
        Compare original and reversed ID columns to verify correctness.

        Performs a detailed comparison of the ID column values before and after
        reverse transformation. Handles NaN values correctly and provides
        detailed mismatch information.

        Args:
            original_df: Filtered and cast original DataFrame (before transformation)
            reversed_df: Filtered and cast reversed DataFrame (after reverse transformation)
            deid_ref_dict: Dictionary of de-identification reference DataFrames (not used in comparison)

        Returns:
            List of ColumnComparisonResult objects containing:
            - Comparison status (pass/error)
            - Mismatch count and percentage
            - Sample mismatch indices
            - Error messages if comparison fails

        """
        column_name = self.idconfig.name

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

    def _apply_transform(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Transform ID data by replacing original values with de-identified ones.

        This method applies forward transformation (de-identification) by:
        1. Getting or creating deid_ref_df for this transformer's deid column's uid
        2. Updating deid_ref_df with new mappings for any missing values
        3. Joining df with deid_ref_df (inner join) to get mappings
        4. Replacing original column values with de-identified values
        5. Updating deid_ref_dict with the new/updated mappings

        Args:
            df: DataFrame containing the data to transform (after filtering and casting)
            deid_ref_dict: Dictionary of deidentification reference DataFrames, keyed by transformer UID

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        Raises:
            ValueError: If ID column is not in df.columns
            ValueError: If deid_ref_df is missing or invalid
            ValueError: If some values don't have deid mappings after processing

        """
        return self._apply_deid(df, deid_ref_dict, reverse=False)

    def _apply_reverse(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Reverse the ID de-identification by mapping de-identified values back to original values.

        This method applies reverse transformation (restoration) by:
        1. Getting the existing deid_ref_df from deid_ref_dict (must exist)
        2. Joining df with deid_ref_df to map de-identified values back to original values
        3. Replacing the de-identified column with original values
        4. Returning deid_ref_dict unchanged (no new mappings created in reverse mode)

        Args:
            df: DataFrame containing the de-identified data to reverse
            deid_ref_dict: Dictionary of deidentification reference DataFrames, keyed by transformer UID

        Returns:
            Tuple of (reversed_df, updated_deid_ref_dict)
            Note: updated_deid_ref_dict is unchanged in reverse mode

        Raises:
            ValueError: If de-identified column is not in df.columns
            ValueError: If deid_ref_df is not found or doesn't have required columns
            ValueError: If some values in df don't have mappings in deid_ref_df

        """
        return self._apply_deid(df, deid_ref_dict, reverse=True)

    def _apply_deid(
        self,
        df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
        reverse: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Apply de-identification to the DataFrame.

        This is the core method that handles both forward transformation (de-identify)
        and reverse transformation (restore original values). It performs the following steps:
        1. Validates that the ID column exists in the DataFrame
        2. Gets or creates the de-identification reference DataFrame
        3. Merges the DataFrame with the reference to get mappings
        4. Validates the merge was successful
        5. Replaces column values with de-identified/original values
        6. Updates the deid_ref_dict with new mappings (forward mode only)

        Args:
            df: DataFrame containing the data to transform or reverse
            deid_ref_dict: Dictionary of deidentification reference DataFrames, keyed by transformer UID
            reverse: If True, reverse the transformation (restore original values).
                    If False, apply transformation (replace with de-identified values).

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        Raises:
            ValueError: If ID column is not found in DataFrame
            ValueError: If deid_ref_df is missing or invalid
            ValueError: If merge validation fails (missing mappings, type mismatches, etc.)

        """
        # Validate input
        if self.idconfig.name not in df.columns:
            error_msg = f"Column '{self.idconfig.name}' not found in DataFrame"
            raise ValueError(error_msg)

        # Get or create deid_ref_df for this transformer's deid  column's uid

        deid_ref_df = (
            self._get_and_update_deid_mappings(df, deid_ref_dict)
            if not reverse
            else deid_ref_dict.get(self.idconfig.uid)
        )
        self._validate_apply_deid_args(deid_ref_df)

        # Inner join to ensure all values have mappings (raises error if some don't)
        # Use suffixes to handle overlapping column names (e.g., when both df and deid_ref_df have 'user_id')
        merged = df.merge(
            deid_ref_df[[self.idconfig.uid, self.idconfig.deid_uid()]],
            left_on=self.idconfig.name,
            right_on=self.idconfig.uid if not reverse else self.idconfig.deid_uid(),
            how="inner",
            suffixes=("_left", "_right"),
        )

        self._validate_merged_table(merged, df, deid_ref_df)

        # Replace the column values with deidentified/original values
        merged = self._replace_column_values(merged, reverse)

        # Drop the reference columns that were added during merge
        columns_to_drop = self._get_columns_to_drop(merged)
        if len(columns_to_drop) > 0:
            merged.drop(columns=columns_to_drop, inplace=True)

        # Update the deid_ref_dict with the new/updated deid_ref_df/ unchanged
        updated_deid_ref_dict = deid_ref_dict.copy()
        if not reverse:
            updated_deid_ref_dict[self.idconfig.uid] = deid_ref_df.copy()

        return merged, updated_deid_ref_dict

    def _get_columns_to_drop(self, merged: pd.DataFrame) -> list[str]:
        """
        Get list of reference columns to drop from merged DataFrame.

        After merging with deid_ref_df, temporary reference columns are added.
        This method identifies which columns should be dropped, handling cases
        where pandas may have added suffixes (e.g., "_right") due to overlapping
        column names.

        Args:
            merged: The merged DataFrame after joining with deid_ref_df

        Returns:
            List of column names to drop, including:
            - The deid column (deid_uid)
            - The uid column (if different from the original column name)

        """
        columns_to_drop = []
        deid_col = self.idconfig.deid_uid()
        if deid_col not in merged.columns:
            deid_col = f"{self.idconfig.deid_uid()}_right"
        if deid_col in merged.columns:
            columns_to_drop.append(deid_col)

        if self.idconfig.uid != self.idconfig.name:
            uid_col = self.idconfig.uid
            if uid_col not in merged.columns:
                uid_col = f"{self.idconfig.uid}_right"
            if uid_col in merged.columns:
                columns_to_drop.append(uid_col)

        return columns_to_drop

    def _replace_column_values(
        self, merged: pd.DataFrame, reverse: bool
    ) -> pd.DataFrame:
        """
        Replace column values with deidentified/original values, handling pandas suffixes.

        After merging, the de-identified or original values are in separate columns.
        This method replaces the original column with these values, handling cases
        where pandas may have added suffixes due to overlapping column names.

        Args:
            merged: The merged DataFrame after joining with deid_ref_df
            reverse: If True, reverse mode (use uid column to restore original values).
                    If False, forward mode (use deid_uid column to apply de-identified values).

        Returns:
            DataFrame with the original column replaced with de-identified or original values

        Raises:
            ValueError: If required column (deid_uid or uid) not found in merged DataFrame

        """
        # Handle case where pandas added suffixes due to overlapping column names
        if not reverse:
            # Forward mode: use deid_uid column (may have suffix if column name overlaps)
            deid_col = self.idconfig.deid_uid()
            if deid_col not in merged.columns:
                # Try with suffix (pandas adds _right suffix when column names overlap)
                deid_col = f"{self.idconfig.deid_uid()}_right"
            if deid_col not in merged.columns:
                error_msg = f"Column '{self.idconfig.deid_uid()}' not found in merged DataFrame after merge"
                logger.error(f"Transformer {self.uid} {error_msg}")
                raise ValueError(error_msg)
            merged[self.idconfig.name] = merged[deid_col]
        else:
            # Reverse mode: use uid column (may have suffix if column name overlaps)
            uid_col = self.idconfig.uid
            if uid_col not in merged.columns:
                # Try with suffix (pandas adds _right suffix when column names overlap)
                uid_col = f"{self.idconfig.uid}_right"
            if uid_col not in merged.columns:
                error_msg = f"Column '{self.idconfig.uid}' not found in merged DataFrame after merge"
                logger.error(f"Transformer {self.uid} {error_msg}")
                raise ValueError(error_msg)
            merged[self.idconfig.name] = merged[uid_col]
        return merged

    def _validate_merged_table(
        self, merged: pd.DataFrame, df: pd.DataFrame, deid_map: pd.DataFrame
    ) -> None:
        """
        Validate that the merged table has the same number of rows as the original DataFrame.

        Performs comprehensive checks to diagnose why merge might have failed, including
        type mismatches, missing values, duplicate mappings, and other issues.

        Args:
            merged: The merged DataFrame after joining with deid_ref_df
            df: The original DataFrame before merging
            deid_map: The de-identification reference DataFrame used for merging

        Raises:
            ValueError: If merged DataFrame has different number of rows than original DataFrame,
                       with detailed error messages explaining the cause

        """
        merged_rows = merged.shape[0]
        original_rows = df.shape[0]

        # Early return if lengths match
        if self._check_row_count_match(merged_rows, original_rows):
            return

        # Get column names for analysis
        df_col = self.idconfig.name
        deid_map_col = self.idconfig.uid

        # Case 1: Merged has fewer rows (most common - missing mappings)
        if merged_rows < original_rows:
            self._validate_fewer_rows_case(merged, df, deid_map, df_col, deid_map_col)

        # Case 2: Merged has more rows (unusual - duplicate mappings or join issue)
        elif merged_rows > original_rows:
            self._validate_more_rows_case(merged, df, deid_map, df_col, deid_map_col)

    def _check_row_count_match(self, merged_rows: int, original_rows: int) -> bool:
        """
        Check if merged and original row counts match.

        Args:
            merged_rows: Number of rows in merged DataFrame
            original_rows: Number of rows in original DataFrame

        Returns:
            True if row counts match, False otherwise

        """
        return merged_rows == original_rows

    def _get_missing_values_from_merge(
        self, merged: pd.DataFrame, df: pd.DataFrame, df_col: str
    ) -> set:
        """
        Get values that exist in the original DataFrame but not in the merged result.

        Args:
            merged: The merged DataFrame after joining
            df: The original DataFrame before merging
            df_col: Name of the column being de-identified

        Returns:
            Set of values that are missing from the merge result

        """
        df_values = set(df[df_col].dropna().unique())
        merged_values = set(merged[df_col].dropna().unique())
        return df_values - merged_values

    def _check_type_mismatch(
        self, df: pd.DataFrame, deid_map: pd.DataFrame, df_col: str, deid_map_col: str
    ) -> bool:
        """
        Check if there is a type mismatch between DataFrame column and deid_map column.

        Args:
            df: The original DataFrame
            deid_map: The de-identification reference DataFrame
            df_col: Name of the column in DataFrame
            deid_map_col: Name of the column in deid_map

        Returns:
            True if types don't match, False otherwise

        """
        deid_map_dtype = deid_map[deid_map_col].dtype
        df_col_dtype = df[df_col].dtype
        return deid_map_dtype != df_col_dtype

    def _build_type_mismatch_error(
        self,
        df_col: str,
        deid_map_col: str,
        df_col_dtype: type,
        deid_map_dtype: type,
        missing_count: int,
        missing_values: set,
    ) -> str:
        """
        Build error message for type mismatch cases.

        Args:
            df_col: Name of the DataFrame column
            deid_map_col: Name of the deid_map column
            df_col_dtype: Data type of the DataFrame column
            deid_map_dtype: Data type of the deid_map column
            missing_count: Number of rows lost during merge
            missing_values: Set of missing values

        Returns:
            Formatted error message string

        """
        sample_missing = list(missing_values)[:5]
        return (
            f"Some values in '{df_col}' don't have deid mappings. "
            f"{missing_count} row(s) were lost during merge.\n"
            f"This appears to be caused by a type mismatch: "
            f"DataFrame column '{df_col}' has type {df_col_dtype}, "
            f"but deid_map column '{deid_map_col}' has type {deid_map_dtype}.\n"
            f"Missing values (sample): {sample_missing}\n"
            f"Consider using 'value_cast: \"integer\"' (or appropriate type) "
            f"in your transformer configuration to ensure type consistency."
        )

    def _build_missing_mappings_error(
        self, df_col: str, missing_count: int, missing_values: set
    ) -> str:
        """
        Build error message for missing mappings (non-type-mismatch cases).

        Args:
            df_col: Name of the DataFrame column
            missing_count: Number of rows lost during merge
            missing_values: Set of missing values

        Returns:
            Formatted error message string

        """
        sample_missing = list(missing_values)[:5]
        return (
            f"Some values in '{df_col}' don't have deid mappings. "
            f"{missing_count} row(s) were lost during merge.\n"
            f"Missing values (sample): {sample_missing}"
        )

    def _check_duplicates_in_deid_map(
        self, deid_map: pd.DataFrame, deid_map_col: str
    ) -> tuple[bool, list]:
        """
        Check for duplicate values in the deid_map column.

        Args:
            deid_map: The de-identification reference DataFrame
            deid_map_col: Name of the column to check

        Returns:
            Tuple of (has_duplicates: bool, sample_duplicates: list)

        """
        duplicate_mask = deid_map[deid_map_col].duplicated(keep=False)
        has_duplicates = duplicate_mask.any()
        if has_duplicates:
            duplicates = deid_map[deid_map_col][duplicate_mask].unique()[:5]
            return True, list(duplicates)
        return False, []

    def _check_duplicates_in_dataframe(
        self, df: pd.DataFrame, df_col: str
    ) -> tuple[bool, list]:
        """
        Check for duplicate values in the DataFrame column.

        Args:
            df: The original DataFrame
            df_col: Name of the column to check

        Returns:
            Tuple of (has_duplicates: bool, sample_duplicates: list)

        """
        df_duplicates = df[df_col].duplicated(keep=False)
        has_duplicates = df_duplicates.any()
        if has_duplicates:
            duplicate_values = df[df_col][df_duplicates].unique()[:5]
            return True, list(duplicate_values)
        return False, []

    def _build_duplicate_deid_map_error(
        self, extra_count: int, deid_map_col: str, df_col: str, duplicates: list
    ) -> str:
        """
        Build error message for duplicate values in deid_map.

        Args:
            extra_count: Number of extra rows created
            deid_map_col: Name of the deid_map column
            df_col: Name of the DataFrame column
            duplicates: List of sample duplicate values

        Returns:
            Formatted error message string

        """
        return (
            f"Merge resulted in {extra_count} extra row(s). "
            f"This is likely caused by duplicate values in the deid_map.\n"
            f"Duplicate values found in '{deid_map_col}' (sample): {duplicates}\n"
            f"Each value in '{df_col}' should map to exactly one de-identified value."
        )

    def _build_duplicate_dataframe_error(
        self, extra_count: int, df_col: str, duplicate_values: list
    ) -> str:
        """
        Build error message for duplicate values in DataFrame column.

        Args:
            extra_count: Number of extra rows created
            df_col: Name of the DataFrame column
            duplicate_values: List of sample duplicate values

        Returns:
            Formatted error message string

        """
        return (
            f"Merge resulted in {extra_count} extra row(s). "
            f"This may be caused by duplicate values in '{df_col}' "
            f"matching multiple rows in deid_map.\n"
            f"Duplicate values in '{df_col}' (sample): {duplicate_values}"
        )

    def _build_unexpected_extra_rows_error(self, extra_count: int) -> str:
        """
        Build error message for unexpected extra rows (unknown cause).

        Args:
            extra_count: Number of extra rows created

        Returns:
            Formatted error message string

        """
        return (
            f"Merge resulted in {extra_count} extra row(s). "
            f"This is unexpected - the merge should not create more rows than the original DataFrame."
        )

    def _validate_fewer_rows_case(
        self,
        merged: pd.DataFrame,
        df: pd.DataFrame,
        deid_map: pd.DataFrame,
        df_col: str,
        deid_map_col: str,
    ) -> None:
        """
        Validate and handle the case where merged DataFrame has fewer rows than original.

        This typically indicates missing mappings or type mismatches.

        Args:
            merged: The merged DataFrame
            df: The original DataFrame
            deid_map: The de-identification reference DataFrame
            df_col: Name of the DataFrame column
            deid_map_col: Name of the deid_map column

        Raises:
            ValueError: With detailed error message explaining the cause

        """
        missing_count = df.shape[0] - merged.shape[0]
        missing_values = self._get_missing_values_from_merge(merged, df, df_col)

        if self._check_type_mismatch(df, deid_map, df_col, deid_map_col):
            deid_map_dtype = deid_map[deid_map_col].dtype
            df_col_dtype = df[df_col].dtype
            error_msg = self._build_type_mismatch_error(
                df_col,
                deid_map_col,
                df_col_dtype,
                deid_map_dtype,
                missing_count,
                missing_values,
            )
        else:
            error_msg = self._build_missing_mappings_error(
                df_col, missing_count, missing_values
            )

        logger.error(f"Transformer {self.uid} {error_msg}")
        raise ValueError(error_msg)

    def _validate_more_rows_case(
        self,
        merged: pd.DataFrame,
        df: pd.DataFrame,
        deid_map: pd.DataFrame,
        df_col: str,
        deid_map_col: str,
    ) -> None:
        """
        Validate and handle the case where merged DataFrame has more rows than original.

        This typically indicates duplicate mappings causing a cartesian product.

        Args:
            merged: The merged DataFrame
            df: The original DataFrame
            deid_map: The de-identification reference DataFrame
            df_col: Name of the DataFrame column
            deid_map_col: Name of the deid_map column

        Raises:
            ValueError: With detailed error message explaining the cause

        """
        extra_count = merged.shape[0] - df.shape[0]

        has_duplicates, duplicates = self._check_duplicates_in_deid_map(
            deid_map, deid_map_col
        )
        if has_duplicates:
            error_msg = self._build_duplicate_deid_map_error(
                extra_count, deid_map_col, df_col, duplicates
            )
        else:
            has_df_duplicates, duplicate_values = self._check_duplicates_in_dataframe(
                df, df_col
            )
            if has_df_duplicates:
                error_msg = self._build_duplicate_dataframe_error(
                    extra_count, df_col, duplicate_values
                )
            else:
                error_msg = self._build_unexpected_extra_rows_error(extra_count)

        logger.error(f"Transformer {self.uid} {error_msg}")
        raise ValueError(error_msg)

    def _validate_apply_deid_args(self, deid_ref_df: pd.DataFrame | None) -> None:
        """
        Validate that deid_ref_df exists and has required columns.

        Checks that:
        1. deid_ref_df is not None
        2. deid_ref_df contains the deid column (deid_uid)
        3. deid_ref_df contains the uid column

        Args:
            deid_ref_df: The de-identification reference DataFrame to validate

        Raises:
            ValueError: If deid_ref_df is None or missing required columns

        """
        if deid_ref_df is None:
            error_msg = f"De-identification reference not found for transformer {self.uid or 'unnamed'} and identifier {self.idconfig.name}"
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

        if self.idconfig.deid_uid() not in deid_ref_df.columns:
            error_msg = f"Deid column '{self.idconfig.deid_uid()}' not found in deid_ref_df for transformer {self.uid or 'unnamed'} and identifier {self.idconfig.name}"
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

        if self.idconfig.uid not in deid_ref_df.columns:
            error_msg = f"UID column '{self.idconfig.uid}' not found in deid_ref_df for transformer {self.uid or 'unnamed'} and identifier {self.idconfig.name}"
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

    def _get_and_update_deid_mappings(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Get and update deid mappings for the identifier.

        This method:
        1. Retrieves existing deid_ref_df from deid_ref_dict (or creates empty one)
        2. Validates that required columns exist
        3. Identifies values in df that don't have mappings yet
        4. Generates new mappings for missing values
        5. Returns updated deid_ref_df with all mappings

        The method ensures type consistency by converting unique_values to match
        the type of existing values in deid_ref_df.

        Args:
            df: DataFrame containing the data to transform
            deid_ref_dict: Dictionary of deidentification reference DataFrames, keyed by transformer UID

        Returns:
            Updated deid_ref_df with all mappings (existing + newly generated)

        Raises:
            ValueError: If required columns are missing from deid_ref_df

        """
        deid_ref_df = deid_ref_dict.get(
            self.idconfig.uid,
            pd.DataFrame(
                {
                    self.idconfig.uid: pd.Series(dtype="int64"),
                    self.idconfig.deid_uid(): pd.Series(dtype="int64"),
                }
            ),
        )
        if self.idconfig.deid_uid() not in deid_ref_df.columns:
            error_msg = f"Deid column '{self.idconfig.deid_uid()}' not found in deid_ref_df for transformer {self.uid or 'unnamed'} and identifier {self.idconfig.name}"
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

        if self.idconfig.uid not in deid_ref_df.columns:
            error_msg = f"UID of the identifier column '{self.idconfig.uid}' not found in deid_ref_df for transformer {self.uid or 'unnamed'}"
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

        # Get unique values from the reference column
        unique_values = df[self.idconfig.name].dropna().unique()

        # Find values that don't have deid mappings
        existing_values = set(deid_ref_df[self.idconfig.uid].dropna().unique())
        missing_values = set(unique_values) - existing_values

        if missing_values:
            # Generate new deidentified values for missing mappings
            if deid_ref_df.empty:
                last_used_deid_uid = 0
            else:
                # Get the maximum numeric value from existing de-identified values
                deid_values = deid_ref_df[self.idconfig.deid_uid()]
                # Convert to numeric, coercing errors to NaN, then get max
                numeric_values = pd.to_numeric(deid_values, errors="coerce")
                last_used_deid_uid = (
                    0 if numeric_values.isna().all() else int(numeric_values.max())
                )

            new_mappings = self._generate_deid_mappings(
                new_values=list(missing_values), last_used_deid_uid=last_used_deid_uid
            )
            deid_ref_df = pd.concat([deid_ref_df, new_mappings], ignore_index=True)
            logger.debug(
                f"Transformer {self.uid} generated {len(missing_values)} new deid mappings for column '{self.idconfig.name}'"
            )

        return deid_ref_df

    def _generate_deid_mappings(
        self, new_values: list, last_used_deid_uid: int = 0
    ) -> pd.DataFrame:
        """
        Generate deidentification mappings for given values.

        Creates a DataFrame mapping original values to sequential integer de-identified
        values, starting from last_used_deid_uid + 1.

        Args:
            new_values: List of original values to create mappings for
            last_used_deid_uid: Last used de-identified UID to continue from.
                              New mappings will start from this value + 1.

        Returns:
            DataFrame with two columns:
            - self.idconfig.uid: Original values
            - self.idconfig.deid_uid(): Sequential integer de-identified values

        """
        # Generate sequential integer values starting from last_used_deid_uid + 1
        new_deid_uids = np.arange(
            last_used_deid_uid + 1, last_used_deid_uid + len(new_values) + 1
        )

        # Create mapping DataFrame
        mappings = pd.DataFrame(
            {self.idconfig.uid: new_values, self.idconfig.deid_uid(): new_deid_uids}
        )

        return mappings
