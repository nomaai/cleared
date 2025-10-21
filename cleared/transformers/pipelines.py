"""
Pipeline classes for data de-identification workflows.

This module provides specialized pipeline classes that handle data loading
and de-identification workflows with different scopes and configurations.
"""

import pandas as pd

from .base import Pipeline, BaseTransformer
from ..io import BaseDataLoader
from ..config.structure import IOConfig, DeIDConfig, PairedIOConfig


class TablePipeline(Pipeline):
    """
    Pipeline for processing a single table with data loading capabilities.

    This pipeline extends the base Pipeline class to handle data loading
    from various sources (file system, SQL databases) based on configuration.
    The pipeline reads the table data during the transform operation and
    applies the configured transformers.

    """

    def __init__(
        self,
        table_name: str,
        io_config: PairedIOConfig,
        deid_config: DeIDConfig,
        uid: str | None = None,
        dependencies: list[str] | None = None,
        transformers: list[BaseTransformer] | None = None,
    ):
        """
        Initialize the table pipeline.

        Args:
            table_name: Name of the table to process
            io_config: Paired IO configuration for data loading
            deid_config: De-identification configuration
            uid: Unique identifier for the pipeline
            dependencies: List of dependency UIDs
            transformers: List of transformer configurations

        """
        super().__init__(uid=uid, transformers=transformers, dependencies=dependencies)
        self.table_name = table_name
        self.io_config = io_config
        self.deid_config = deid_config

    def _create_data_loader(self, io_config: IOConfig) -> BaseDataLoader:
        """
        Create the appropriate data loader based on IO configuration.

        Returns:
            Configured data loader instance

        Raises:
            ValueError: If unsupported IO type is specified

        """
        from ..io import create_data_loader

        return create_data_loader(io_config)

    def transform(
        self,
        df: pd.DataFrame | None = None,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Transform the table data.

        If no DataFrame is provided, the pipeline will read the table
        using the configured data loader. Otherwise, it will process
        the provided DataFrame.

        Args:
            df: Optional input DataFrame. If None, table will be read from data source
            deid_ref_dict: Optional dictionary of de-identification reference DataFrames, keys are the UID of the transformers that created the reference

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        Raises:
            ValueError: If table cannot be read and no DataFrame is provided

        """
        # Read table if no DataFrame provided
        if df is None:
            try:
                with self._create_data_loader(
                    self.io_config.input_config
                ) as data_loader:
                    df = data_loader.read_table(self.table_name)
            except Exception as e:
                raise ValueError(
                    f"Failed to read table '{self.table_name}': {e!s}"
                ) from e

        # Build empty de-identification reference if not provided
        if deid_ref_dict is None:
            deid_ref_dict = {}

        # Process with base pipeline
        df, deid_ref_dict = super().transform(df, deid_ref_dict)

        # Write de-identified data to the data source
        with self._create_data_loader(self.io_config.output_config) as data_loader:
            data_loader.write_deid_table(df, self.table_name)

        return df, deid_ref_dict
