"""
Pipeline classes for data de-identification workflows.

This module provides specialized pipeline classes that handle data loading
and de-identification workflows with different scopes and configurations.
"""

from __future__ import annotations

import pandas as pd
import logging
from pathlib import Path

from .base import Pipeline, BaseTransformer, FormattedDataFrameError
from ..io import BaseDataLoader, TableNotFoundError, create_data_loader
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
        return create_data_loader(io_config)

    def _transform_segment(
        self,
        segment_path: Path,
        segment_name: str | None,
        deid_ref_dict: dict[str, pd.DataFrame],
        read_config: IOConfig,
        output_config: IOConfig | None,
        rows_limit: int | None = None,
        test_mode: bool = False,
        reverse: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Transform a single segment file.

        Args:
            segment_path: Path to the segment file to read
            segment_name: Name of segment file (for output naming, None if single file)
            deid_ref_dict: De-identification reference dictionary (will be updated)
            read_config: IOConfig to use for reading
            output_config: IOConfig to use for writing (None in test_mode)
            rows_limit: Optional row limit
            test_mode: Skip writing outputs
            reverse: If True, run in reverse mode

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        """
        logger.debug(
            f"    Processing segment: {segment_path.name if segment_name else 'single file'}"
        )

        # Read segment file
        with self._create_data_loader(read_config) as data_loader:
            df = data_loader.read_table(
                self.table_name, rows_limit=rows_limit, segment_path=segment_path
            )
        if segment_name:
            logger.info(f"        Read segment '{segment_name}' ({len(df)} rows)")
        else:
            logger.debug(f"    Read segment ({len(df)} rows)")

        # Process with base pipeline (use reverse() if in reverse mode)
        if reverse:
            df, deid_ref_dict = super().reverse(df, deid_ref_dict, test_mode=test_mode)
        else:
            df, deid_ref_dict = super().transform(
                df, deid_ref_dict, test_mode=test_mode
            )

        # Log segment completion in test mode
        if test_mode and segment_name:
            logger.info(f"        Completed segment '{segment_name}' ({len(df)} rows)")

        # Write segment if not in test mode
        if not test_mode and output_config is not None:
            with self._create_data_loader(output_config) as data_loader:
                data_loader.write_deid_table(
                    df, self.table_name, segment_name=segment_name
                )
            if segment_name:
                logger.info(f"        Wrote segment '{segment_name}' ({len(df)} rows)")
            else:
                logger.debug(f"    Wrote segment ({len(df)} rows)")

        return df, deid_ref_dict

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

        Supports both single files and directories of segment files.
        For directories, processes each segment separately and returns
        a combined DataFrame.

        Args:
            df: Optional input DataFrame. If None, table will be read from data source
            deid_ref_dict: Optional dictionary of de-identification reference DataFrames, keys are the UID of the transformers that created the reference
            rows_limit: Optional limit on number of rows to read (for testing)
            test_mode: If True, skip writing outputs (dry run mode)

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)
            - For single files: single transformed DataFrame
            - For segments: combined DataFrame from all segments

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

    def _detect_table_structure(
        self, read_config: IOConfig
    ) -> tuple[Path | list[Path], bool]:
        """
        Detect if table is a single file or directory of segments.

        Args:
            read_config: IOConfig to use for reading

        Returns:
            Tuple of (table_paths, is_segments)
            - table_paths: Path for single file, or list[Path] for segments
            - is_segments: True if directory, False if single file

        Raises:
            TableNotFoundError: If table doesn't exist
            ValueError: If detection fails

        """
        try:
            with self._create_data_loader(read_config) as data_loader:
                table_paths = data_loader.get_table_paths(self.table_name)
        except TableNotFoundError:
            # Re-raise TableNotFoundError as-is so engine can detect and skip if configured
            raise
        except Exception as e:
            error_msg = (
                f"Failed to detect table structure for '{self.table_name}': {e!s}"
            )
            logger.error(f"    {error_msg}")
            raise ValueError(error_msg) from e

        is_segments = isinstance(table_paths, list)
        return table_paths, is_segments

    def _process_single_file_table(
        self,
        table_path: Path,
        deid_ref_dict: dict[str, pd.DataFrame],
        read_config: IOConfig,
        output_config: IOConfig | None,
        rows_limit: int | None,
        test_mode: bool,
        reverse: bool,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Process a single file table."""
        logger.debug(f"    Table '{self.table_name}' is a single file")
        return self._transform_segment(
            table_path,
            None,  # segment_name is None for single file
            deid_ref_dict,
            read_config,
            output_config,
            rows_limit,
            test_mode,
            reverse,
        )

    def _process_segment_directory(
        self,
        segment_paths: list[Path],
        deid_ref_dict: dict[str, pd.DataFrame],
        read_config: IOConfig,
        output_config: IOConfig | None,
        rows_limit: int | None,
        test_mode: bool,
        reverse: bool,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Process a directory of segment files."""
        logger.info(
            f"    Table '{self.table_name}' is a directory with {len(segment_paths)} segment(s)"
        )

        if not segment_paths:
            logger.warning(
                f"    Table directory '{self.table_name}' is empty, returning empty DataFrame"
            )
            return pd.DataFrame(), deid_ref_dict

        # Process each segment
        all_dataframes = []
        for idx, segment_path in enumerate(segment_paths):
            segment_name = segment_path.name
            logger.info(
                f"      Processing segment {idx + 1}/{len(segment_paths)}: {segment_name}"
            )
            try:
                segment_df, deid_ref_dict = self._transform_segment(
                    segment_path,
                    segment_name,
                    deid_ref_dict,
                    read_config,
                    output_config,
                    rows_limit,
                    test_mode,
                    reverse,
                )
                all_dataframes.append(segment_df)
            except Exception as e:
                # Re-raise with context
                error_msg = f"Failed to process segment '{segment_name}': {e!s}"
                logger.error(f"    {error_msg}")
                raise ValueError(error_msg) from e

        # Combine all segments
        if all_dataframes:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            logger.info(
                f"    Combined {len(segment_paths)} segment(s) into {len(combined_df)} total rows"
            )
            return combined_df, deid_ref_dict
        else:
            return pd.DataFrame(), deid_ref_dict

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

        # Build empty de-identification reference if not provided
        if deid_ref_dict is None:
            deid_ref_dict = {}

        # If DataFrame is provided, process it directly (no segment detection)
        if df is not None:
            # Determine output config for single dataframe processing
            output_config_for_df = (
                reverse_output_config if reverse else self.io_config.output_config
            )
            return self._process_single_dataframe(
                df, deid_ref_dict, test_mode, reverse, output_config_for_df
            )

        # Detect table structure
        table_paths, is_segments = self._detect_table_structure(read_config)

        # Determine output config
        output_config = (
            reverse_output_config if reverse else self.io_config.output_config
        )

        # Process based on structure
        if is_segments:
            return self._process_segment_directory(
                table_paths,
                deid_ref_dict,
                read_config,
                output_config,
                rows_limit,
                test_mode,
                reverse,
            )
        else:
            return self._process_single_file_table(
                table_paths,
                deid_ref_dict,
                read_config,
                output_config,
                rows_limit,
                test_mode,
                reverse,
            )

    def _process_single_dataframe(
        self,
        df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
        test_mode: bool,
        reverse: bool,
        output_config: IOConfig | None,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Process a single DataFrame (when df parameter is provided).

        Args:
            df: DataFrame to process
            deid_ref_dict: De-identification reference dictionary
            test_mode: Skip writing outputs
            reverse: If True, run in reverse mode
            output_config: IOConfig for writing (None in test_mode)

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        """
        # Process with base pipeline (use reverse() if in reverse mode)
        try:
            if reverse:
                df, deid_ref_dict = super().reverse(
                    df, deid_ref_dict, test_mode=test_mode
                )
            else:
                df, deid_ref_dict = super().transform(
                    df, deid_ref_dict, test_mode=test_mode
                )
        except (ValueError, KeyError, AttributeError) as e:
            self._handle_dataframe_error(e)

        # Write data to the appropriate location (skip in test mode)
        if not test_mode and output_config is not None:
            logger.debug(f"    Writing table '{self.table_name}' to output")
            with self._create_data_loader(output_config) as data_loader:
                data_loader.write_deid_table(df, self.table_name)
            logger.info(f"    Wrote table '{self.table_name}' ({len(df)} rows)")

        return df, deid_ref_dict

    def _handle_dataframe_error(self, error: Exception) -> None:
        """
        Handle DataFrame-related errors with proper formatting.

        Args:
            error: The exception that occurred

        Raises:
            FormattedDataFrameError: If error is already formatted
            ValueError: If error needs table context added

        """
        error_str = str(error)
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
            if "Missing Column Error" in error_str or "\n" in error_str:
                # Already formatted, don't add prefix
                raise FormattedDataFrameError(error_str) from error
            else:
                # Not formatted yet, add table context
                enhanced_error = (
                    f"Error processing table '{self.table_name}': {error!s}"
                )
                raise ValueError(enhanced_error) from error
        # Re-raise other errors as-is
        raise

    def _create_temp_loader_config(self, base_path: Path) -> IOConfig:
        """
        Create temporary IOConfig with overridden base_path.

        Args:
            base_path: Path to override in config

        Returns:
            New IOConfig with overridden base_path

        """
        input_config = self.io_config.input_config
        loader_config = input_config.configs.copy()
        loader_config["base_path"] = str(base_path)
        return IOConfig(
            io_type=input_config.io_type,
            suffix=input_config.suffix,
            configs=loader_config,
        )

    def _load_table_data(
        self,
        data_path: Path,
        rows_limit: int | None = None,
    ) -> pd.DataFrame:
        """
        Load table data from path (handles both single file and segments).

        Args:
            data_path: Path to directory containing data
            rows_limit: Optional limit on number of rows to read

        Returns:
            Combined DataFrame (single file or combined segments)

        Raises:
            ValueError: If data cannot be loaded

        """
        temp_config = self._create_temp_loader_config(data_path)
        temp_loader = self._create_data_loader(temp_config)

        try:
            table_paths = temp_loader.get_table_paths(self.table_name)
        except Exception as e:
            error_msg = f"Failed to load data for '{self.table_name}': {e!s}"
            logger.error(f"    {error_msg}")
            raise ValueError(error_msg) from e

        if isinstance(table_paths, Path):
            # Single file
            return temp_loader.read_table(
                self.table_name, rows_limit=rows_limit, segment_path=table_paths
            )
        else:
            # Directory of segments - combine all segments
            segment_dfs = []
            for segment_path in table_paths:
                segment_df = temp_loader.read_table(
                    self.table_name, rows_limit=rows_limit, segment_path=segment_path
                )
                segment_dfs.append(segment_df)
            if segment_dfs:
                return pd.concat(segment_dfs, ignore_index=True)
            else:
                return pd.DataFrame()

    def compare(
        self,
        original_data_path: Path,
        reversed_data_path: Path,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
        rows_limit: int | None = None,
    ) -> list[ColumnComparisonResult]:
        """
        Compare original and reversed table data.

        Supports both single files and directories of segment files.
        For directories, combines all segments before comparison.

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

        # Load original and reversed data
        original_df = self._load_table_data(original_data_path, rows_limit)
        reversed_df = self._load_table_data(reversed_data_path, rows_limit)

        # Call parent Pipeline's compare method
        return super().compare(original_df, reversed_df, deid_ref_dict)

    def _create_reverse_output_config(
        self,
        read_config: IOConfig,
        reverse_output_path: Path,
    ) -> IOConfig:
        """
        Create IOConfig for reverse output path.

        Args:
            read_config: IOConfig to base settings on
            reverse_output_path: Path for reverse output

        Returns:
            New IOConfig with base_path set to reverse_output_path

        """
        reverse_output_config = IOConfig(
            io_type=read_config.io_type,
            suffix=read_config.suffix,
            configs=read_config.configs.copy(),
        )
        reverse_output_config.configs["base_path"] = str(reverse_output_path)
        return reverse_output_config

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
            reverse_output_config = self._create_reverse_output_config(
                read_config, reverse_output_path
            )
        else:
            # Normal mode: read from input config
            read_config = io_config.input_config
            reverse_output_config = None

        return read_config, reverse_output_config
