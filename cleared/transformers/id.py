"""Transformers for id."""

from __future__ import annotations

import pandas as pd
import numpy as np
from cleared.transformers.base import FilterableTransformer
from cleared.config.structure import IdentifierConfig, FilterConfig, DeIDConfig


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
            raise ValueError("idconfig is required for IDDeidentifier")

    def _get_column_to_cast(self) -> str | None:
        """Get the column name to cast (the ID column being de-identified)."""
        return self.idconfig.name if self.idconfig else None

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
            raise ValueError(f"Column '{self.idconfig.name}' not found in DataFrame")

        # Get or create deid_ref_df for this transformer's deid  column's uid

        deid_ref_df = (
            self._get_and_update_deid_mappings(df, deid_ref_dict)
            if not reverse
            else deid_ref_dict.get(self.idconfig.uid)
        )
        if deid_ref_df is None:
            raise ValueError(
                f"De-identification reference not found for transformer {self.uid} and identifier {self.idconfig.name}"
            )

        if self.idconfig.deid_uid() not in deid_ref_df.columns:
            raise ValueError(
                f"Deid column '{self.idconfig.deid_uid()}' not found in deid_ref_df for transformer {self.uid} and identifier {self.idconfig.name}"
            )

        if self.idconfig.uid not in deid_ref_df.columns:
            raise ValueError(
                f"UID column '{self.idconfig.uid}' not found in deid_ref_df for transformer {self.uid} and identifier {self.idconfig.name}"
            )

        # Inner join to ensure all values have mappings (raises error if some don't)
        merged = df.merge(
            deid_ref_df[[self.idconfig.uid, self.idconfig.deid_uid()]],
            left_on=self.idconfig.name,
            right_on=self.idconfig.uid if not reverse else self.idconfig.deid_uid(),
            how="inner",
        )

        if merged.shape[0] != df.shape[0]:
            raise ValueError(
                f"Some values in '{self.idconfig.name}' don't have deid mappings"
            )

        # Replace the column values with deidentified/original values
        merged[self.idconfig.name] = (
            merged[self.idconfig.deid_uid()]
            if not reverse
            else merged[self.idconfig.uid]
        )

        # Drop the reference columns that were added during merge
        columns_to_drop = [self.idconfig.deid_uid()]
        if self.idconfig.uid != self.idconfig.name:
            columns_to_drop.append(self.idconfig.uid)

        if columns_to_drop is not None and len(columns_to_drop) > 0:
            merged.drop(columns=columns_to_drop, inplace=True)

        # Update the deid_ref_dict with the new/updated deid_ref_df/ unchanged
        updated_deid_ref_dict = deid_ref_dict.copy()
        if not reverse:
            updated_deid_ref_dict[self.idconfig.uid] = deid_ref_df.copy()

        return merged, updated_deid_ref_dict

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
            raise ValueError(
                f"Deid column '{self.idconfig.deid_uid()}' not found in deid_ref_df for transformer {self.uid} and identifier {self.idconfig.name}"
            )

        if self.idconfig.uid not in deid_ref_df.columns:
            raise ValueError(
                f"UID of the identifier column '{self.idconfig.uid}' not found in deid_ref_df for transformer {self.uid}"
            )

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
