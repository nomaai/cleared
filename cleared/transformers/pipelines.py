"""
Pipeline classes for data de-identification workflows.

This module provides specialized pipeline classes that handle data loading
and de-identification workflows with different scopes and configurations.
"""

from __future__ import annotations

import pandas as pd
import logging
from pathlib import Path

from .base import Pipeline, BaseTransformer
from ..io import BaseDataLoader, TableNotFoundError
from ..config.structure import IOConfig, DeIDConfig, PairedIOConfig
from ..models.verify_models import ColumnComparisonResult

# Set up logger for this module
logger = logging.getLogger(__name__)


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
        rows_limit: int | None = None,
        test_mode: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Transform the table data.

        If no DataFrame is provided, the pipeline will read the table
        using the configured data loader. Otherwise, it will process
        the provided DataFrame.

        Args:
            df: Optional input DataFrame. If None, table will be read from data source
            deid_ref_dict: Optional dictionary of de-identification reference DataFrames, keys are the UID of the transformers that created the reference
            rows_limit: Optional limit on number of rows to read (for testing)
            test_mode: If True, skip writing outputs (dry run mode)

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        Raises:
            ValueError: If table cannot be read and no DataFrame is provided

        """
        return self._run_pipeline(
            df,
            deid_ref_dict,
            rows_limit,
            test_mode,
            reverse=False,
            reverse_output_path=None,
        )

    def reverse(
        self,
        df: pd.DataFrame | None = None,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
        rows_limit: int | None = None,
        test_mode: bool = False,
        reverse_output_path: str | Path | None = None,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Reverse the table data.

        Args:
            df: Optional input DataFrame. If None, table will be read from data source
            deid_ref_dict: Optional dictionary of de-identification reference DataFrames
            rows_limit: Optional limit on number of rows to read (for testing)
            test_mode: If True, skip writing outputs (dry run mode)
            reverse_output_path: Directory path for reverse mode output (required)

        Returns:
            Tuple of (reversed_df, updated_deid_ref_dict)

        """
        return self._run_pipeline(
            df,
            deid_ref_dict,
            rows_limit,
            test_mode,
            reverse=True,
            reverse_output_path=reverse_output_path,
        )

    def _run_pipeline(
        self,
        df: pd.DataFrame | None = None,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
        rows_limit: int | None = None,
        test_mode: bool = False,
        reverse: bool = False,
        reverse_output_path: str | Path | None = None,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Run the pipeline."""
        # Prepare IO configuration for reverse mode
        read_config, reverse_output_config = self._prepare_reverse_io_config(
            self.io_config, reverse, reverse_output_path
        )

        # Read table if no DataFrame provided
        if df is None:
            logger.debug(f"    Reading table '{self.table_name}' from data source")
            try:
                with self._create_data_loader(read_config) as data_loader:
                    df = data_loader.read_table(self.table_name, rows_limit=rows_limit)
                logger.info(f"    Read table '{self.table_name}' ({len(df)} rows)")
            except TableNotFoundError:
                # Re-raise TableNotFoundError as-is so engine can detect and skip if configured
                raise
            except Exception as e:
                error_msg = f"Failed to read table '{self.table_name}': {e!s}"
                logger.error(f"    {error_msg}")
                raise ValueError(error_msg) from e

        # Build empty de-identification reference if not provided
        if deid_ref_dict is None:
            deid_ref_dict = {}

        # Process with base pipeline (use reverse() if in reverse mode)
        try:
            if reverse:
                df, deid_ref_dict = super().reverse(df, deid_ref_dict)
            else:
                df, deid_ref_dict = super().transform(df, deid_ref_dict)
        except (ValueError, KeyError, AttributeError) as e:
            # Check if it's a DataFrame-related error
            error_str = str(e)
            error_lower = error_str.lower()
            if any(
                keyword in error_lower
                for keyword in [
                    "column",
                    "not found",
                    "dataframe",
                    "index",
                    "key",
                    "missing",
                ]
            ):
                # If error is already formatted (contains "Missing Column Error" or has newlines), re-raise as-is
                # Otherwise, add table context
                from .base import FormattedDataFrameError

                if "Missing Column Error" in error_str or "\n" in error_str:
                    # Already formatted, don't add prefix
                    raise FormattedDataFrameError(error_str) from e
                else:
                    # Not formatted yet, add table context
                    enhanced_error = (
                        f"Error processing table '{self.table_name}': {e!s}"
                    )
                    raise ValueError(enhanced_error) from e
            # Re-raise other errors as-is
            raise

        # Write data to the appropriate location (skip in test mode)
        if not test_mode:
            if reverse:
                # Write to reverse output path
                if reverse_output_config is None:
                    error_msg = "reverse_output_config is required when reverse=True"
                    logger.error(f"    {error_msg}")
                    raise ValueError(error_msg)
                logger.debug(
                    f"    Writing reversed table '{self.table_name}' to reverse output path"
                )
                with self._create_data_loader(reverse_output_config) as data_loader:
                    data_loader.write_deid_table(df, self.table_name)
                logger.info(
                    f"    Wrote reversed table '{self.table_name}' ({len(df)} rows)"
                )
            else:
                # Write to normal output config
                logger.debug(f"    Writing table '{self.table_name}' to output")
                with self._create_data_loader(
                    self.io_config.output_config
                ) as data_loader:
                    data_loader.write_deid_table(df, self.table_name)
                logger.info(f"    Wrote table '{self.table_name}' ({len(df)} rows)")

        return df, deid_ref_dict

    def compare(
        self,
        original_data_path: Path,
        reversed_data_path: Path,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
        rows_limit: int | None = None,
    ) -> list[ColumnComparisonResult]:
        """
        Compare original and reversed table data.

        Args:
            original_data_path: Path to directory containing original data
            reversed_data_path: Path to directory containing reversed data
            deid_ref_dict: Dictionary of de-identification reference DataFrames (optional)
            rows_limit: Optional limit on number of rows to read (for testing)

        Returns:
            List of ColumnComparisonResult objects, one per transformer

        Raises:
            ValueError: If tables cannot be read

        """
        if deid_ref_dict is None:
            deid_ref_dict = {}

        # Load original data
        original_config = self.io_config.input_config
        # Override base_path temporarily by creating a new IOConfig
        original_loader_config = original_config.configs.copy()
        original_loader_config["base_path"] = str(original_data_path)
        temp_original_config = IOConfig(
            io_type=original_config.io_type,
            suffix=original_config.suffix,
            configs=original_loader_config,
        )
        temp_original_loader = self._create_data_loader(temp_original_config)
        original_df = temp_original_loader.read_table(
            self.table_name, rows_limit=rows_limit
        )

        # Load reversed data
        reversed_config = self.io_config.input_config
        # Override base_path temporarily by creating a new IOConfig
        reversed_loader_config = reversed_config.configs.copy()
        reversed_loader_config["base_path"] = str(reversed_data_path)
        temp_reversed_config = IOConfig(
            io_type=reversed_config.io_type,
            suffix=reversed_config.suffix,
            configs=reversed_loader_config,
        )
        temp_reversed_loader = self._create_data_loader(temp_reversed_config)
        reversed_df = temp_reversed_loader.read_table(
            self.table_name, rows_limit=rows_limit
        )

        # Call parent Pipeline's compare method
        return super().compare(original_df, reversed_df, deid_ref_dict)

    def _prepare_reverse_io_config(
        self,
        io_config: PairedIOConfig,
        reverse: bool,
        reverse_output_path: str | Path | None,
    ) -> tuple[IOConfig, IOConfig | None]:
        """
        Prepare IO configuration for reverse mode.

        Args:
            io_config: Paired IO configuration for data loading
            reverse: If True, run in reverse mode (read from output config, write to reverse path)
            reverse_output_path: Directory path for reverse mode output (required if reverse=True)

        Returns:
            Tuple of (read_config, reverse_output_config)
            - read_config: IOConfig to use for reading data
            - reverse_output_config: IOConfig to use for writing reversed data (None if not reverse mode)

        Raises:
            ValueError: If reverse_output_path is required but not provided

        """
        if reverse:
            # Read from output config (where de-identified data is)
            read_config = io_config.output_config
            # Create reverse output config pointing to reverse_output_path
            if reverse_output_path is None:
                error_msg = "reverse_output_path is required when reverse=True"
                logger.error(f"Pipeline {self.uid} {error_msg}")
                raise ValueError(error_msg)

            reverse_output_path = Path(reverse_output_path)
            reverse_output_path.mkdir(parents=True, exist_ok=True)

            # Create a new IOConfig for reverse output with the same settings as output but different path
            reverse_output_config = IOConfig(
                io_type=read_config.io_type,
                suffix=read_config.suffix,
                configs=read_config.configs.copy(),
            )

            # Update base_path to point to reverse_output_path
            if "base_path" in reverse_output_config.configs:
                reverse_output_config.configs["base_path"] = str(reverse_output_path)
            else:
                reverse_output_config.configs["base_path"] = str(reverse_output_path)
        else:
            # Normal mode: read from input config
            read_config = io_config.input_config
            reverse_output_config = None

        return read_config, reverse_output_config
