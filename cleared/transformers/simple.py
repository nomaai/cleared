"""Simple transformers for basic data operations."""

from __future__ import annotations

import pandas as pd
from cleared.transformers.base import BaseTransformer
from cleared.config.structure import IdentifierConfig


class ColumnDropper(BaseTransformer):
    """Transformer to drop a column from a DataFrame."""

    def __init__(
        self,
        idconfig: IdentifierConfig | dict,
        uid: str | None = None,
        dependencies: list[str] | None = None,
    ):
        """
        Drop a column from a DataFrame.

        Args:
            idconfig (IdentifierConfig or dict): Configuration for the column to drop
            uid (str, optional): Unique identifier for the transformer
            dependencies (list[str], optional): List of dependency UIDs

        """
        super().__init__(uid=uid, dependencies=dependencies)

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
            raise ValueError(f"Column '{self.idconfig.name}' not found in DataFrame")

        # Create a copy of the DataFrame and drop the specified column
        result_df = df.drop(columns=[self.idconfig.name])

        # Return the transformed DataFrame and unchanged deid_ref_dict
        return result_df, deid_ref_dict.copy() if deid_ref_dict is not None else None
