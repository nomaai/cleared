"""Simple transformers for basic data operations."""

from __future__ import annotations

import pandas as pd
import logging
from cleared.transformers.base import BaseTransformer
from cleared.config.structure import IdentifierConfig, DeIDConfig
from cleared.models.verify_models import ColumnComparisonResult

# Set up logger for this module
logger = logging.getLogger(__name__)

# Set up logger for this module
logger = logging.getLogger(__name__)


class ColumnDropper(BaseTransformer):
    """Transformer to drop a column from a DataFrame."""

    def __init__(
        self,
        idconfig: IdentifierConfig | dict,
        uid: str | None = None,
        dependencies: list[str] | None = None,
        global_deid_config: DeIDConfig | None = None,
    ):
        """
        Drop a column from a DataFrame.

        Args:
            idconfig (IdentifierConfig or dict): Configuration for the column to drop
            uid (str, optional): Unique identifier for the transformer
            dependencies (list[str], optional): List of dependency UIDs
            global_deid_config: Global de-identification configuration (optional)

        """
        super().__init__(
            uid=uid, dependencies=dependencies, global_deid_config=global_deid_config
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
            raise ValueError("idconfig is required for ColumnDropper")

    def transform(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Drop the specified column from the DataFrame.

        Args:
            df: Input DataFrame
            deid_ref_dict: Dictionary of reference DataFrames (not used for this transformer)

        Returns:
            Tuple of (transformed DataFrame, unchanged deid_ref_dict)

        Raises:
            ValueError: If the column to drop is not found in the DataFrame

        """
        # Validate input
        if self.idconfig.name not in df.columns:
            error_msg = f"Column '{self.idconfig.name}' not found in DataFrame"
            logger.error(f"Transformer {self.uid} {error_msg}")
            raise ValueError(error_msg)

        # Create a copy of the DataFrame and drop the specified column
        logger.debug(f"Transformer {self.uid} dropping column '{self.idconfig.name}'")
        result_df = df.drop(columns=[self.idconfig.name])

        # Return the transformed DataFrame and unchanged deid_ref_dict
        return result_df, deid_ref_dict.copy() if deid_ref_dict is not None else None

    def reverse(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Reverse the column drop operation.

        Note: Since a dropped column cannot be restored without the original data,
        this method simply returns the DataFrame as-is. In reverse mode, the column
        is already missing from the input DataFrame (read from output), so there's
        nothing to restore.

        Args:
            df: Input DataFrame (column is already missing in reverse mode)
            deid_ref_dict: Dictionary of reference DataFrames (not used for this transformer)

        Returns:
            Tuple of (unchanged DataFrame, unchanged deid_ref_dict)

        """
        # In reverse mode, the column is already dropped, so just return as-is
        return df.copy(), deid_ref_dict.copy() if deid_ref_dict is not None else {}

    def compare(
        self,
        original_df: pd.DataFrame,
        reversed_df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
    ):
        """
        Compare original and reversed DataFrames for ColumnDropper.

        For ColumnDropper, verification means checking that the column is missing
        in the reversed DataFrame (which is expected behavior).

        Args:
            original_df: Original DataFrame before transformation
            reversed_df: Reversed DataFrame after reverse transformation
            deid_ref_dict: Dictionary of de-identification reference DataFrames (not used)

        Returns:
            List of ColumnComparisonResult objects

        """
        column_name = self.idconfig.name

        # Check if column exists in original (it should)
        if column_name not in original_df.columns:
            return [
                ColumnComparisonResult(
                    column_name=column_name,
                    status="error",
                    message=f"Column '{column_name}' not found in original DataFrame (unexpected)",
                    original_length=len(original_df),
                    reversed_length=len(reversed_df),
                    mismatch_count=0,
                    mismatch_percentage=0.0,
                )
            ]

        # Check if column is missing in reversed (it should be)
        if column_name in reversed_df.columns:
            return [
                ColumnComparisonResult(
                    column_name=column_name,
                    status="error",
                    message=f"Column '{column_name}' should be dropped but found in reversed DataFrame",
                    original_length=len(original_df),
                    reversed_length=len(reversed_df),
                    mismatch_count=len(reversed_df),
                    mismatch_percentage=100.0,
                )
            ]

        # Column is correctly dropped - this is expected
        return [
            ColumnComparisonResult(
                column_name=column_name,
                status="warning",
                message=f"Column '{column_name}' is missing in the reversed but this is correct behaviour",
                original_length=len(original_df),
                reversed_length=len(reversed_df),
                mismatch_count=0,
                mismatch_percentage=0.0,
            )
        ]
