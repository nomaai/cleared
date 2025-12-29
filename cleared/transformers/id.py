"""Transformers for id."""

from __future__ import annotations

import pandas as pd
import numpy as np
import logging
from cleared.transformers.base import FilterableTransformer
from cleared.config.structure import IdentifierConfig, FilterConfig, DeIDConfig
from cleared.models.verify_models import ColumnComparisonResult

# Set up logger for this module
logger = logging.getLogger(__name__)

# Set up logger for this module
logger = logging.getLogger(__name__)


class IDDeidentifier(FilterableTransformer):
    """De-identifier for id columns."""

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
        De-identify ID columns in a DataFrame.

        Args:
            idconfig (IdentifierConfig or dict): Configuration for the ID column to de-identify
            filter_config (FilterConfig, optional): Configuration for filtering operations
            value_cast (str, optional): Type to cast the de-identification column to
            uid (str, optional): Unique identifier for the transformer
            dependencies (list[str], optional): List of dependency UIDs
            global_deid_config: Global de-identification configuration (optional)

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
        """Get the column name to cast (the ID column being de-identified)."""
        return self.idconfig.name if self.idconfig else None

    def _compare(
        self,
        original_df: pd.DataFrame,
        reversed_df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
    ):
        """
        Compare original and reversed ID columns to verify correctness.

        Args:
            original_df: Filtered and cast original DataFrame
            reversed_df: Filtered and cast reversed DataFrame
            deid_ref_dict: Dictionary of de-identification reference DataFrames

        Returns:
            List of ColumnComparisonResult objects

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

        This method:
        1. Checks if deid column's uid exists in deid_ref_dict
        2. If not, creates deid_ref_df for this transformer's deid column's uid
        3. If exists, updates it with the new values if any missing
        4. Joins df with deid_ref_df (inner join) to ensure all values have mappings
        5. Replaces original column with deidentified values and drops the deid column

        Args:
            df: DataFrame containing the data to transform
            deid_ref_dict: Dictionary of deidentification reference DataFrames, keyed by transformer UID

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        Raises:
            ValueError: If ref_col is not in df.columns
            ValueError: If some values in df[ref_col] don't have deid mappings after processing

        """
        return self._apply_deid(df, deid_ref_dict, reverse=False)

    def _apply_reverse(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Reverse the ID de-identification by mapping de-identified values back to original values.

        This method:
        1. Gets the deid_ref_df for this transformer's deid column's uid
        2. Joins df with deid_ref_df to map de-identified values back to original values
        3. Replaces the de-identified column with original values

        Args:
            df: DataFrame containing the de-identified data to reverse
            deid_ref_dict: Dictionary of deidentification reference DataFrames, keyed by transformer UID

        Returns:
            Tuple of (reversed_df, updated_deid_ref_dict)

        Raises:
            ValueError: If deid column is not in df.columns
            ValueError: If deid_ref_df is not found or doesn't have required columns
            ValueError: If some values in df don't have mappings

        """
        return self._apply_deid(df, deid_ref_dict, reverse=True)

    def _apply_deid(
        self,
        df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
        reverse: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Apply de-identification to the DataFrame."""
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

        self._validate_merged_table(merged, df)

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

        Handles suffixes that pandas may have added when column names overlap.

        Args:
            merged: The merged DataFrame after joining with deid_ref_df

        Returns:
            List of column names to drop

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

        Args:
            merged: The merged DataFrame after joining with deid_ref_df
            reverse: If True, reverse mode (use uid column). If False, forward mode (use deid_uid column).

        Returns:
            DataFrame with column values replaced

        Raises:
            ValueError: If required column not found in merged DataFrame

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

    def _validate_merged_table(self, merged: pd.DataFrame, df: pd.DataFrame) -> None:
        """
        Validate that the merged table has the same number of rows as the original DataFrame.

        Args:
            merged: The merged DataFrame after joining with deid_ref_df
            df: The original DataFrame before merging

        Raises:
            ValueError: If merged DataFrame has different number of rows than original DataFrame

        """
        if merged.shape[0] != df.shape[0]:
            error_msg = (
                f"Some values in '{self.idconfig.name}' don't have deid mappings"
            )
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

    def _validate_apply_deid_args(self, deid_ref_df: pd.DataFrame | None) -> None:
        """
        Validate that deid_ref_df exists and has required columns.

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

        Args:
            df: DataFrame containing the data to transform
            deid_ref_dict: Dictionary of deidentification reference DataFrames, keyed by transformer UID

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

        Args:
            new_values: List of original values to create mappings for
            last_used_deid_uid: Last used de-identified UID to continue from

        Returns:
            DataFrame with original and deidentified value mappings

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
