"""
ClearedEngine for orchestrating data de-identification workflows.

This module provides the main engine class that coordinates multiple pipelines
and manages the overall de-identification process.
"""

from __future__ import annotations

from typing import Any, Literal
from pathlib import Path
import pandas as pd
from dataclasses import dataclass, field
import os
import glob
import json
import logging

from datetime import datetime
from .config.structure import (
    DeIDConfig,
    ClearedIOConfig,
    ClearedConfig,
    TransformerConfig,
)
from .transformers.pipelines import TablePipeline
from .transformers.base import Pipeline, FilterableTransformer, FormattedDataFrameError
from .transformers.registry import TransformerRegistry
from .io.base import TableNotFoundError


# Set up logger for this module
logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result from a pipeline execution."""

    status: Literal["success", "error", "skipped"]
    error: str | None = None


@dataclass
class Results:
    """Results from ClearedEngine execution."""

    success: bool = True
    results: dict[str, PipelineResult] = field(default_factory=dict)
    execution_order: list[str] = field(default_factory=list)

    def add_pipeline_result(
        self, pipeline_uid: str, status: str, error: str | None = None
    ) -> None:
        """
        Add a pipeline result to the results.

        Args:
            pipeline_uid: UID of the pipeline
            status: Status of the pipeline execution ('success', 'error', 'skipped')
            error: Error message if status is 'error'

        """
        self.results[pipeline_uid] = PipelineResult(status=status, error=error)

    def add_execution_order(self, pipeline_uid: str) -> None:
        """
        Add a pipeline to the execution order.

        Args:
            pipeline_uid: UID of the pipeline

        """
        self.execution_order.append(pipeline_uid)

    def set_success(self, success: bool) -> None:
        """
        Set the overall success status.

        Args:
            success: Whether the execution was successful

        """
        self.success = success

    def has_errors(self) -> bool:
        """
        Check if there are any errors in the results.

        Returns:
            True if any pipeline has an error status, False otherwise

        """
        return any(result.status == "error" for result in self.results.values())

    def get_error_count(self) -> int:
        """
        Get the number of pipelines that failed.

        Returns:
            Number of pipelines with error status

        """
        return sum(1 for result in self.results.values() if result.status == "error")

    def get_successful_pipelines(self) -> list[str]:
        """
        Get list of successfully executed pipeline UIDs.

        Returns:
            List of pipeline UIDs that executed successfully

        """
        return [
            uid for uid, result in self.results.items() if result.status == "success"
        ]

    def get_failed_pipelines(self) -> list[str]:
        """
        Get list of failed pipeline UIDs.

        Returns:
            List of pipeline UIDs that failed

        """
        return [uid for uid, result in self.results.items() if result.status == "error"]


class ClearedEngine:
    """
    Main engine for orchestrating data de-identification workflows.

    The ClearedEngine coordinates multiple pipelines and manages the overall
    de-identification process. It can run pipelines sequentially and maintain
    shared state across pipeline executions.

    Attributes:
        pipelines: List of Pipeline instances to execute
        deid_config: DeIDConfig instance for de-identification settings
        registry: TransformerRegistry instance for transformer management
        io_config: ClearedIOConfig instance for I/O operations
        uid: Unique identifier for this engine instance
        results: Dictionary storing results from pipeline executions

    """

    def __init__(
        self,
        name: str,
        deid_config: DeIDConfig,
        io_config: ClearedIOConfig,
        pipelines: list[TablePipeline] | None = None,
        registry: TransformerRegistry | None = None,
    ):
        """
        Initialize the ClearedEngine.

        Args:
            name: Name of the engine
            pipelines: List of Pipeline instances to execute. Defaults to empty list.
            deid_config: DeIDConfig instance for de-identification settings.
                        Defaults to empty DeIDConfig.
            registry: TransformerRegistry instance for transformer management.
                     Defaults to new registry with default transformers.
            io_config: ClearedIOConfig instance for I/O operations.
                      Defaults to None.

        """
        logger.info(f"Initializing ClearedEngine: {name}")
        logger.debug(f"Provided pipelines: {len(pipelines) if pipelines else 0}")
        logger.debug(f"Registry provided: {registry is not None}")

        self._pipelines = pipelines if pipelines is not None else []
        self._registry = (
            registry if registry is not None else TransformerRegistry(use_defaults=True)
        )
        self._setup_and_validate(name, deid_config, io_config)

        logger.info(
            f"ClearedEngine '{name}' initialized successfully with UID: {self._uid}"
        )
        logger.debug(f"Total pipelines configured: {len(self._pipelines)}")

    @classmethod
    def from_config(
        cls, config: ClearedConfig, registry: TransformerRegistry | None = None
    ) -> ClearedEngine:
        """
        Create a ClearedEngine from a configuration.

        Args:
            config: ClearedConfig instance to initialize from
            registry: Optional TransformerRegistry instance

        Returns:
            ClearedEngine instance configured from the provided config

        """
        logger.info(f"Creating ClearedEngine from config: {config.name}")
        logger.debug(f"Config contains {len(config.tables)} table(s)")

        engine = cls.__new__(cls)
        engine._init_from_config(config, registry)

        logger.info(
            f"ClearedEngine created from config with {len(engine._pipelines)} pipeline(s)"
        )
        return engine

    def _setup_and_validate(
        self,
        name: str,
        deid_config: DeIDConfig,
        io_config: ClearedIOConfig,
        skip_missing_tables: bool = True,
    ) -> None:
        """Set the properties of the engine."""
        logger.debug(f"Setting up engine properties for: {name}")
        self.name = name
        self.deid_config = deid_config
        self.io_config = io_config
        self.skip_missing_tables = skip_missing_tables
        self.results: dict[str, Any] = {}
        self._uid = f"{name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        logger.debug(f"Generated engine UID: {self._uid}")
        logger.debug(f"Skip missing tables: {skip_missing_tables}")
        logger.debug("Validating IO configuration...")
        self._validate_io_config()
        logger.debug("IO configuration validation passed")

    def _init_from_config(
        self, config: ClearedConfig, registry: TransformerRegistry | None = None
    ) -> None:
        """
        Initialize the engine from a configuration.

        Args:
            config: Configuration to initialize from
            registry: TransformerRegistry to use. Defaults to new registry with default transformers.

        """
        logger.debug("Initializing engine from configuration")
        # Use getattr with default True for backward compatibility with older configs
        skip_missing = getattr(config, "skip_missing_tables", True)
        self._setup_and_validate(
            config.name, config.deid_config, config.io, skip_missing
        )
        self._registry = (
            registry if registry is not None else TransformerRegistry(use_defaults=True)
        )
        logger.info(f"Loading {len(config.tables)} pipeline(s) from configuration")
        self._pipelines = self._load_pipelines_from_config(config)
        logger.info(f"Loaded {len(self._pipelines)} pipeline(s) from configuration")

    def _create_transformer_config_dict(
        self, transformer_config: TransformerConfig
    ) -> dict[str, Any]:
        """
        Create a configuration dictionary for transformer instantiation.

        This method builds the complete config dict including filter_config and value_cast
        if the transformer class supports them.

        Args:
            transformer_config: Transformer configuration from the pipeline config

        Returns:
            Dictionary of configuration arguments for transformer instantiation

        """
        logger.debug(
            f"Creating config dict for transformer: {transformer_config.method}"
        )
        # Start with the base configs
        config_dict = {**transformer_config.configs}

        # Get transformer class from registry to check for properties
        transformer_class = self._registry.get_class(transformer_config.method)

        # Check if class inherits from FilterableTransformer (which supports filter_config and value_cast)
        # Handle case where get_class might return a mock in tests
        try:
            supports_filter_and_cast = isinstance(
                transformer_class, type
            ) and issubclass(transformer_class, FilterableTransformer)
        except (TypeError, AttributeError):
            # If transformer_class is not a real class (e.g., a mock), default to False
            supports_filter_and_cast = False
            logger.debug(
                f"Could not determine if {transformer_config.method} supports filter/cast, defaulting to False"
            )

        # Add filter_config if present and class supports it
        if transformer_config.filter is not None:
            if supports_filter_and_cast:
                logger.debug(f"Adding filter_config to {transformer_config.method}")
                config_dict["filter_config"] = transformer_config.filter
            else:
                error_msg = f"Transformer {transformer_config.method} does not support filter_config but it was provided!"
                logger.error(error_msg)
                raise ValueError(error_msg)

        # Add value_cast if present and class supports it
        if transformer_config.value_cast is not None:
            if supports_filter_and_cast:
                logger.debug(f"Adding value_cast to {transformer_config.method}")
                config_dict["value_cast"] = transformer_config.value_cast
            else:
                error_msg = f"Transformer {transformer_config.method} does not support value_cast but it was provided!"
                logger.error(error_msg)
                raise ValueError(error_msg)

        logger.debug(
            f"Config dict created for {transformer_config.method} with {len(config_dict)} key(s)"
        )
        return config_dict

    def _load_pipelines_from_config(self, config: ClearedConfig) -> list[TablePipeline]:
        """
        Load the pipelines from the configuration.

        Args:
            config: Configuration to load pipelines from

        """
        pipelines = []
        for table_name, table_config in config.tables.items():
            logger.info(
                f"  → Loading pipeline: {table_name} ({len(table_config.transformers)} transformer(s))"
            )
            pip = TablePipeline(
                table_name, config.io.data, config.deid_config, uid=table_name
            )
            for idx, transformer_config in enumerate(table_config.transformers):
                logger.debug(
                    f"    Adding transformer {idx + 1}/{len(table_config.transformers)}: {transformer_config.method}"
                )
                # Create complete config dict including filter_config and value_cast
                config_dict = self._create_transformer_config_dict(transformer_config)

                # Create transformer with complete configs and global_deid_config
                transformer = self._registry.instantiate(
                    transformer_config.method,
                    config_dict,
                    uid=transformer_config.uid,
                    global_deid_config=config.deid_config,
                )

                pip.add_transformer(transformer)
                logger.debug(
                    f"    Transformer '{transformer_config.method}' added successfully"
                )

            pipelines.append(pip)
            logger.info(f"  ✓ Pipeline '{table_name}' loaded (UID: {pip.uid})")
        return pipelines

    def _log_execution_start(
        self,
        reverse: bool,
        test_mode: bool,
        continue_on_error: bool,
        rows_limit: int | None,
    ) -> None:
        """Log execution start information."""
        mode_str = "REVERSE" if reverse else "NORMAL"
        row_limit_str = f", row_limit={rows_limit}" if rows_limit else ""
        logger.info(
            f"Starting ClearedEngine execution (UID: {self._uid}, mode={mode_str}, test_mode={test_mode}, continue_on_error={continue_on_error}{row_limit_str})"
        )

    def _log_execution_summary(self, results: Results) -> None:
        """Log execution summary."""
        successful_count = len(results.get_successful_pipelines())
        failed_count = results.get_error_count()
        logger.info(
            f"Execution completed: {len(results.execution_order)} total, {successful_count} successful, {failed_count} failed, overall_success={results.success}"
        )

    def run(
        self,
        continue_on_error: bool = False,
        rows_limit: int | None = None,
        test_mode: bool = False,
        reverse: bool = False,
        reverse_output_path: str | Path | None = None,
    ) -> dict[str, Any]:
        """
        Run all pipelines sequentially.

        This method executes each pipeline in the order they were added,
        passing the output of one pipeline as input to the next. The
        de-identification reference dictionary is shared across all pipelines.

        Args:
            continue_on_error: If True, continue running remaining pipelines even if one fails.
                             If False, stop on first error.
            rows_limit: Optional limit on number of rows to read per table (for testing)
            test_mode: If True, skip writing outputs (dry run mode)
            reverse: If True, run in reverse mode (read from output config, write to reverse path)
            reverse_output_path: Directory path for reverse mode output (required if reverse=True)

        Returns:
            Dictionary containing:
            - 'success': Boolean indicating if all pipelines completed successfully
            - 'results': Dictionary of pipeline results keyed by pipeline UID
            - 'execution_order': List of pipeline UIDs in execution order

        Raises:
            ValueError: If no pipelines are configured
            RuntimeError: If pipeline execution fails and continue_on_error is False

        """
        self._log_execution_start(reverse, test_mode, continue_on_error, rows_limit)

        if self._pipelines is None or len(self._pipelines) == 0:
            logger.error("No pipelines configured")
            raise ValueError("No pipelines configured. Add pipelines before running.")

        logger.info(f"Executing {len(self._pipelines)} pipeline(s)")

        # Initialize de-identification reference dictionary
        logger.debug("Loading initial de-identification reference dictionary")
        current_deid_ref_dict = self._load_initial_deid_ref_dict()
        # Note: _load_initial_deid_ref_dict already logs the total count

        # Initialize results
        results = Results()

        # Execute each pipeline
        for idx, table_pipeline in enumerate(self._pipelines):
            logger.info(
                f"  → Pipeline {idx + 1}/{len(self._pipelines)}: {table_pipeline.uid}"
            )
            current_deid_ref_dict = self._run_table_pipeline(
                table_pipeline,
                results,
                current_deid_ref_dict,
                continue_on_error,
                rows_limit=rows_limit,
                test_mode=test_mode,
                reverse=reverse,
                reverse_output_path=reverse_output_path,
            )

        # Store results in instance
        self.results = results

        # Skip saving outputs in test mode
        if not test_mode:
            logger.debug("Saving execution results")
            self._save_results(results)
            logger.debug("Saving de-identification reference files")
            # Save de-identification reference files
            self._save_deid_ref_files(current_deid_ref_dict)
        else:
            logger.info("  Test mode: Skipping output file writes")

        self._log_execution_summary(results)
        return results

    def verify(
        self,
        original_data_path: Path,
        reversed_data_path: Path,
        rows_limit: int | None = None,
    ) -> dict[str, Any]:
        """
        Verify reversed data against original data by comparing using transformers.

        This method loads original and reversed data for each table and uses
        the transformers to compare them, ensuring the same filtering and casting
        logic is applied as during transformation.

        Args:
            original_data_path: Path to directory containing original data
            reversed_data_path: Path to directory containing reversed data
            rows_limit: Optional limit on number of rows to read per table (for testing)

        Returns:
            Dictionary containing verification results for each table

        Raises:
            ValueError: If no pipelines are configured

        """
        logger.info(f"Starting verification (UID: {self._uid})")

        if self._pipelines is None or len(self._pipelines) == 0:
            logger.error("No pipelines configured")
            raise ValueError("No pipelines configured. Add pipelines before verifying.")

        logger.info(f"Verifying {len(self._pipelines)} pipeline(s)")

        # Load de-identification reference dictionary (needed for comparison)
        logger.debug("Loading de-identification reference dictionary")
        deid_ref_dict = self._load_initial_deid_ref_dict()
        logger.info(f"Loaded {len(deid_ref_dict)} de-identification reference file(s)")

        # Verify each pipeline
        from cleared.models.verify_models import ColumnComparisonResult

        table_results: dict[str, list[ColumnComparisonResult]] = {}
        for idx, table_pipeline in enumerate(self._pipelines):
            logger.info(
                f"  → Verifying pipeline {idx + 1}/{len(self._pipelines)}: {table_pipeline.uid}"
            )
            try:
                result = table_pipeline.compare(
                    original_data_path=Path(original_data_path),
                    reversed_data_path=Path(reversed_data_path),
                    deid_ref_dict=deid_ref_dict,
                    rows_limit=rows_limit,
                )
                table_results[table_pipeline.uid] = result

                # Log summary
                passed_count = sum(1 for r in result if r.status == "pass")
                error_count = sum(1 for r in result if r.status == "error")
                warning_count = sum(1 for r in result if r.status == "warning")

                if error_count == 0 and warning_count == 0:
                    logger.info(
                        f"  ✓ Pipeline {table_pipeline.uid} verification passed ({passed_count} transformer(s))"
                    )
                else:
                    logger.warning(
                        f"  ⚠ Pipeline {table_pipeline.uid} verification: {passed_count} passed, {error_count} errors, {warning_count} warnings"
                    )
            except Exception as e:
                error_msg = f"Pipeline {table_pipeline.uid} verification failed: {e!s}"
                logger.error(f"  ✗ {error_msg}")
                table_results[table_pipeline.uid] = [
                    ColumnComparisonResult(
                        column_name="pipeline_error",
                        status="error",
                        message=error_msg,
                        original_length=0,
                        reversed_length=0,
                        mismatch_count=0,
                        mismatch_percentage=0.0,
                    )
                ]

        # Calculate overall statistics
        total_tables = len(table_results)
        passed_tables = sum(
            1
            for results in table_results.values()
            if all(r.status == "pass" for r in results)
        )
        failed_tables = sum(
            1
            for results in table_results.values()
            if any(r.status == "error" for r in results)
        )
        warning_tables = sum(
            1
            for results in table_results.values()
            if any(r.status == "warning" for r in results)
            and not any(r.status == "error" for r in results)
        )

        overall_status = (
            "pass"
            if failed_tables == 0 and warning_tables == 0
            else "error"
            if failed_tables > 0
            else "warning"
        )

        logger.info(
            f"Verification completed: {total_tables} total, {passed_tables} passed, {failed_tables} failed, {warning_tables} warnings, overall_status={overall_status}"
        )

        return {
            "overall_status": overall_status,
            "total_tables": total_tables,
            "passed_tables": passed_tables,
            "failed_tables": failed_tables,
            "warning_tables": warning_tables,
            "table_results": table_results,
        }

    def _run_table_pipeline(
        self,
        table_pipeline: TablePipeline,
        results: Results,
        current_deid_ref_dict: dict[str, pd.DataFrame],
        continue_on_error: bool,
        rows_limit: int | None = None,
        test_mode: bool = False,
        reverse: bool = False,
        reverse_output_path: str | Path | None = None,
    ) -> dict[str, pd.DataFrame]:
        """
        Run a table pipeline and update the de-identification reference dictionary.

        Args:
               table_pipeline: TablePipeline to run
               results: Results to store
               current_deid_ref_dict: Current de-identification reference dictionary
               continue_on_error: Whether to continue execution if this pipeline fails
               rows_limit: Optional limit on number of rows to read per table (for testing)
               test_mode: If True, skip writing outputs (dry run mode)
               reverse: If True, run in reverse mode (read from output config, write to reverse path)
               reverse_output_path: Directory path for reverse mode output (required if reverse=True)

        Returns:
            Updated de-identification reference dictionary

        Raises:
            RuntimeError: If pipeline execution fails and continue_on_error is False

        """
        pipeline_uid = table_pipeline.uid
        results.add_execution_order(pipeline_uid)

        logger.debug(f"    Starting execution of pipeline: {pipeline_uid}")
        logger.debug(
            f"    Current deid_ref_dict contains {len(current_deid_ref_dict)} reference(s)"
        )

        try:
            # Run the pipeline
            if hasattr(table_pipeline, "transform") and callable(
                table_pipeline.transform
            ):
                logger.debug(
                    f"    Pipeline {pipeline_uid} has transform method, executing..."
                )
                # Pipeline has transform method - execute it
                _, updated_deid_ref_dict = self._call_pipeline(
                    pipeline=table_pipeline,
                    df=None,
                    deid_ref_dict=current_deid_ref_dict,
                    rows_limit=rows_limit,
                    test_mode=test_mode,
                    reverse=reverse,
                    reverse_output_path=reverse_output_path,
                )

                # Store pipeline result
                results.add_pipeline_result(pipeline_uid, "success")
                logger.info(f"  ✓ Pipeline {pipeline_uid} completed successfully")
                logger.debug(
                    f"    Updated deid_ref_dict now contains {len(updated_deid_ref_dict)} reference(s)"
                )
                return updated_deid_ref_dict
            else:
                error_msg = f"Pipeline {pipeline_uid} does not have a transform method"
                logger.error(f"    {error_msg}")
                results.add_pipeline_result(
                    pipeline_uid,
                    "error",
                    error_msg,
                )
                return current_deid_ref_dict

        except TableNotFoundError as e:
            # Handle missing table files - skip if configured, otherwise error
            error_msg = str(e)
            if self.skip_missing_tables:
                logger.warning(
                    f"  ⏭ Skipping pipeline {pipeline_uid}: table file not found"
                )
                results.add_pipeline_result(pipeline_uid, "skipped", error_msg)
                return current_deid_ref_dict
            else:
                # Treat as error
                results.add_pipeline_result(pipeline_uid, "error", error_msg)
                if not continue_on_error:
                    results.set_success(False)
                    raise ValueError(error_msg) from e
                logger.warning(
                    "    Continuing execution despite pipeline failure (continue_on_error=True)"
                )
                return current_deid_ref_dict

        except Exception as e:
            # Check if it's a formatted DataFrame error
            is_formatted_error = isinstance(e, FormattedDataFrameError)

            if is_formatted_error:
                # For formatted errors, don't log - let CLI handle display
                error_msg = str(e)
            else:
                # Check error type - filter errors should be checked first
                error_str = str(e)
                error_lower = error_str.lower()

                # Check if it's a filter condition error (should be RuntimeError)
                is_filter_error = "invalid filter condition" in error_lower

                # Check if it's a DataFrame-related error (but not a filter error)
                is_dataframe_error = not is_filter_error and any(
                    keyword in error_lower
                    for keyword in [
                        "column",
                        "not found",
                        "dataframe",
                        "index",
                        "key",
                        "missing",
                    ]
                )

                if is_dataframe_error:
                    error_msg = error_str
                elif is_filter_error:
                    # Filter errors should be RuntimeError
                    error_msg = error_str
                else:
                    # For other errors, include more context and traceback
                    error_msg = f"Pipeline {pipeline_uid} failed: {e!s}"
                    logger.error(error_msg, exc_info=True)

            results.add_pipeline_result(pipeline_uid, "error", error_msg)

            # Stop execution if not continuing on error
            if not continue_on_error:
                results.set_success(False)
                # For formatted errors, re-raise as-is (CLI will handle display)
                if is_formatted_error:
                    raise
                elif is_dataframe_error:
                    # Use ValueError so CLI can detect and handle it properly
                    raise ValueError(error_msg) from None
                elif is_filter_error:
                    # Filter errors should be RuntimeError
                    raise RuntimeError(error_msg) from e
                else:
                    raise RuntimeError(f"Pipeline execution failed: {e!s}") from e

            # If continuing on error, return the unchanged deid_ref_dict
            logger.warning(
                "    Continuing execution despite pipeline failure (continue_on_error=True)"
            )
            return current_deid_ref_dict

    def _call_pipeline(
        self,
        pipeline: TablePipeline,
        df: pd.DataFrame | None = None,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
        rows_limit: int | None = None,
        test_mode: bool = False,
        reverse: bool = False,
        reverse_output_path: str | Path | None = None,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Call the appropriate pipeline method (transform or reverse).

        Args:
            pipeline: TablePipeline to execute
            df: Optional input DataFrame
            deid_ref_dict: Dictionary of de-identification reference DataFrames
            rows_limit: Optional limit on number of rows to read
            test_mode: If True, skip writing outputs
            reverse: If True, call reverse() instead of transform()
            reverse_output_path: Directory path for reverse mode output

        Returns:
            Tuple of (result_df, updated_deid_ref_dict)

        """
        if not reverse:
            logger.debug(f"    Calling transform() on pipeline {pipeline.uid}")
            return pipeline.transform(
                df=df,
                deid_ref_dict=deid_ref_dict,
                rows_limit=rows_limit,
                test_mode=test_mode,
            )
        else:
            logger.debug(
                f"    Calling reverse() on pipeline {pipeline.uid}, output_path={reverse_output_path}"
            )
            return pipeline.reverse(
                df=df,
                deid_ref_dict=deid_ref_dict,
                rows_limit=rows_limit,
                test_mode=test_mode,
                reverse_output_path=reverse_output_path,
            )

    def _load_initial_deid_ref_dict(self) -> dict[str, pd.DataFrame]:
        """
        Load the initial de-identification reference dictionary.

        This method reads all CSV files from the deid_ref input directory
        and loads them into a dictionary with the filename (without .csv) as the key.
        Numeric columns are automatically converted to appropriate types (int/float).

        Returns:
            Dictionary of initial de-identification reference DataFrames

        """
        logger.debug("Loading initial de-identification reference dictionary")
        deid_ref_dict = {}
        if self.io_config.deid_ref.input_config is None:
            logger.debug(
                "No deid_ref input config provided, returning empty dictionary"
            )
            return {}

        if self.io_config.deid_ref.input_config.io_type != "filesystem":
            logger.debug(
                f"Deid_ref input io_type is '{self.io_config.deid_ref.input_config.io_type}', not filesystem. Returning empty dictionary"
            )
            return {}

        base_path = self.io_config.deid_ref.input_config.configs.get("base_path")
        if not base_path:
            logger.debug(
                "No base_path in deid_ref input config, returning empty dictionary"
            )
            return {}

        logger.debug(f"Loading deid_ref files from: {base_path}")
        if not os.path.exists(base_path):
            logger.debug(
                f"De-identification reference input directory not found: {base_path}. Returning empty dictionary"
            )
            return {}

        csv_pattern = os.path.join(base_path, "*.csv")
        csv_files = glob.glob(csv_pattern)
        logger.debug(
            f"Found {len(csv_files)} CSV file(s) matching pattern: {csv_pattern}"
        )

        for csv_file in csv_files:
            try:
                # Get filename without extension as the key
                filename = os.path.basename(csv_file)
                key = os.path.splitext(filename)[0]  # Remove .csv extension
                logger.debug(f"Loading deid_ref file: {filename} (key: {key})")

                # Read CSV file
                df = pd.read_csv(csv_file)
                logger.debug(f"Loaded {len(df)} row(s) from {filename}")

                # Convert numeric columns to appropriate types
                df = self._convert_numeric_columns(df)

                deid_ref_dict[key] = df
                logger.info(
                    f"  ✓ Loaded deid_ref: {key} ({len(df)} rows, {len(df.columns)} columns)"
                )

            except Exception as e:
                # Log error but continue with other files
                logger.warning(
                    f"Could not load CSV file {csv_file}: {e}", exc_info=True
                )
                continue

        logger.info(f"Loaded {len(deid_ref_dict)} de-identification reference file(s)")
        return deid_ref_dict

    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert numeric columns to appropriate types (int64/float64).

        This method processes a DataFrame to ensure numeric columns are properly typed:
        - String numbers are converted to int64 if they're whole numbers, float64 otherwise
        - Existing integer columns are standardized to int64
        - Existing float columns are standardized to float64
        - Non-numeric strings are left as object type

        Args:
            df: DataFrame to process

        Returns:
            DataFrame with properly typed numeric columns

        """
        for column in df.columns:
            if df[column].dtype == "object":
                # Try to convert to numeric
                try:
                    # First try to convert to int
                    numeric_series = pd.to_numeric(df[column], errors="coerce")
                    if not numeric_series.isna().any():
                        # All values are numeric, check if they're integers
                        if (numeric_series % 1 == 0).all():
                            df[column] = numeric_series.astype("int64")
                        else:
                            df[column] = numeric_series.astype("float64")
                except (ValueError, TypeError):
                    # Keep as object if conversion fails
                    pass
            elif df[column].dtype in ["int64", "int32", "int16", "int8"]:
                # Ensure integer columns are int64
                df[column] = df[column].astype("int64")
            elif df[column].dtype in ["float64", "float32", "float16"]:
                # Ensure float columns are float64
                df[column] = df[column].astype("float64")

        return df

    def _validate_io_config(self) -> None:
        """
        Validate the IO configuration.

        This method validates the IO configuration to ensure it is properly configured.

        Raises:
            ValueError: If the IO configuration is not properly configured

        """
        logger.debug("Validating IO configuration...")
        if self.io_config is None:
            logger.error("IO Config is None")
            raise ValueError("IO Config is required")

        if self.io_config.deid_ref is None:
            logger.error("De-identification IO config is None")
            raise ValueError(
                "De-identification IO config must contain at least outout io configurations"
            )

        if self.io_config.deid_ref.input_config is not None:
            if self.io_config.deid_ref.input_config.io_type != "filesystem":
                logger.error(
                    f"Deid_ref input io_type is '{self.io_config.deid_ref.input_config.io_type}', expected 'filesystem'"
                )
                raise ValueError(
                    "De-identification reference dictionary input configuration must be of type filesystem"
                )

        if self.io_config.deid_ref.output_config is None:
            logger.error(
                "De-identification reference dictionary output configuration is None"
            )
            raise ValueError(
                "De-identification reference dictionary output configuration must be provided"
            )

        if self.io_config.deid_ref.output_config.io_type != "filesystem":
            logger.error(
                f"Deid_ref output io_type is '{self.io_config.deid_ref.output_config.io_type}', expected 'filesystem'"
            )
            raise ValueError(
                "De-identification reference dictionary output configuration must be of type filesystem"
            )

        if self.io_config.data is None:
            logger.error("Data IO config is None")
            raise ValueError("Data IO config must be provided")

        if self.io_config.data.input_config is None:
            logger.error("Data input configuration is None")
            raise ValueError("Data input configuration must be provided")

        if self.io_config.data.output_config is None:
            logger.error("Data output configuration is None")
            raise ValueError("Data output configuration must be provided")

        logger.debug("IO configuration validation passed")

    def _save_results(self, results: Results) -> None:
        """
        Save the results to the output directory.

        Args:
            results: Results to save

        """
        logger.debug("Preparing to save execution results")
        # Convert Results to dictionary for JSON serialization
        results_dict = {
            "success": results.success,
            "execution_order": results.execution_order,
            "results": {
                uid: {"status": result.status, "error": result.error}
                for uid, result in results.results.items()
            },
        }

        output_file = os.path.join(
            self.io_config.runtime_io_path, f"status_{self._uid}.json"
        )
        logger.debug(f"Saving results to: {output_file}")

        # Ensure directory exists
        os.makedirs(self.io_config.runtime_io_path, exist_ok=True)

        with open(output_file, "w") as f:
            json.dump(results_dict, f, indent=2)

        logger.info(f"  Results saved to: {output_file}")

    def _save_deid_ref_files(self, deid_ref_dict: dict[str, pd.DataFrame]) -> None:
        """
        Save de-identification reference files to the output directory.

        Args:
            deid_ref_dict: Dictionary of de-identification reference DataFrames

        """
        logger.debug("Preparing to save de-identification reference files")
        if self.io_config.deid_ref.output_config is None:
            logger.debug("No deid_ref output config, skipping save")
            return

        if self.io_config.deid_ref.output_config.io_type != "filesystem":
            logger.debug(
                f"Deid_ref output io_type is '{self.io_config.deid_ref.output_config.io_type}', not filesystem. Skipping save"
            )
            return

        base_path = self.io_config.deid_ref.output_config.configs.get("base_path")
        if not base_path:
            logger.debug("No base_path in deid_ref output config, skipping save")
            return

        logger.debug(f"Saving deid_ref files to: {base_path}")
        # Create output directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)
        logger.debug(f"Output directory created/verified: {base_path}")

        # Save each reference DataFrame as CSV
        for ref_name, ref_df in deid_ref_dict.items():
            output_file = os.path.join(base_path, f"{ref_name}.csv")
            logger.debug(
                f"Saving deid_ref '{ref_name}' ({len(ref_df)} rows) to: {output_file}"
            )
            ref_df.to_csv(output_file, index=False)
            logger.info(f"  ✓ Saved deid_ref file: {output_file}")

        logger.info(
            f"Saved {len(deid_ref_dict)} de-identification reference file(s) to {base_path}"
        )

    def add_pipeline(self, pipeline: Pipeline) -> None:
        """
        Add a pipeline to the engine.

        Args:
            pipeline: Pipeline instance to add

        """
        if pipeline is None:
            logger.error("Attempted to add None pipeline")
            raise ValueError("Pipeline cannot be None")
        logger.info(f"Adding pipeline: {pipeline.uid}")
        self._pipelines.append(pipeline)
        logger.debug(f"Total pipelines: {len(self._pipelines)}")

    def remove_pipeline(self, pipeline_uid: str) -> bool:
        """
        Remove a pipeline from the engine by its UID.

        Args:
            pipeline_uid: UID of the pipeline to remove

        Returns:
            True if pipeline was found and removed, False otherwise

        """
        logger.debug(f"Attempting to remove pipeline: {pipeline_uid}")
        for i, pipeline in enumerate(self._pipelines):
            if pipeline.uid == pipeline_uid:
                del self._pipelines[i]
                logger.info(f"Removed pipeline: {pipeline_uid}")
                logger.debug(f"Remaining pipelines: {len(self._pipelines)}")
                return True
        logger.warning(f"Pipeline not found for removal: {pipeline_uid}")
        return False

    def get_pipeline(self, pipeline_uid: str) -> Pipeline | None:
        """
        Get a pipeline by its UID.

        Args:
            pipeline_uid: UID of the pipeline to retrieve

        Returns:
            Pipeline instance if found, None otherwise

        """
        for pipeline in self._pipelines:
            if pipeline.uid == pipeline_uid:
                return pipeline
        return None

    def list_pipelines(self) -> list[str]:
        """
        Get list of pipeline UIDs.

        Returns:
            List of pipeline UIDs

        """
        return [pipeline.uid for pipeline in self._pipelines]

    def get_results(self) -> dict[str, Any]:
        """
        Get the results from the last run.

        Returns:
            Dictionary of results from the last run, or empty dict if no run has been executed

        """
        return self.results.copy()

    def clear_results(self) -> None:
        """Clear stored results."""
        logger.debug("Clearing stored results")
        self.results = {}

    def get_pipeline_count(self) -> int:
        """
        Get the number of configured pipelines.

        Returns:
            Number of pipelines

        """
        return len(self._pipelines)

    def is_empty(self) -> bool:
        """
        Check if the engine has any pipelines configured.

        Returns:
            True if no pipelines are configured, False otherwise

        """
        return len(self._pipelines) == 0

    def get_registry(self) -> TransformerRegistry:
        """
        Get the registry.

        This method returns the registry of the engine.

        Returns:
            TransformerRegistry: The registry of the engine

        """
        return self._registry

    def set_registry(self, registry: TransformerRegistry) -> None:
        """
        Set the registry.

        This method sets the registry of the engine.

        Args:
            registry: TransformerRegistry: The registry to set

        """
        self._registry = registry

    def __repr__(self) -> str:
        """
        Return string representation of the engine.

        Returns:
            String representation

        """
        pipeline_count = len(self._pipelines)
        return (
            f"ClearedEngine(pipelines={pipeline_count}, "
            f"deid_config={self.deid_config is not None}, "
            f"registry={self._registry is not None}, "
            f"io_config={self.io_config is not None}, "
            f"uid={self._uid})"
        )

    def __len__(self) -> int:
        """
        Get the number of pipelines.

        Returns:
            Number of pipelines

        """
        return len(self._pipelines)

    def __bool__(self) -> bool:
        """
        Check if the engine has pipelines.

        Returns:
            True if pipelines are configured, False otherwise

        """
        return len(self._pipelines) > 0
