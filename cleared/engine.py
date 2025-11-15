"""
ClearedEngine for orchestrating data de-identification workflows.

This module provides the main engine class that coordinates multiple pipelines
and manages the overall de-identification process.
"""

from __future__ import annotations

from typing import Any, Literal
import pandas as pd
from dataclasses import dataclass, field
import os
import glob
import json

from datetime import datetime
from .config.structure import (
    DeIDConfig,
    ClearedIOConfig,
    ClearedConfig,
    TransformerConfig,
)
from .transformers.pipelines import TablePipeline
from .transformers.base import Pipeline
from .transformers.registry import TransformerRegistry


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
        self._pipelines = pipelines if pipelines is not None else []
        self._registry = (
            registry if registry is not None else TransformerRegistry(use_defaults=True)
        )
        self._setup_and_validate(name, deid_config, io_config)

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
        engine = cls.__new__(cls)
        engine._init_from_config(config, registry)
        return engine

    def _setup_and_validate(
        self, name: str, deid_config: DeIDConfig, io_config: ClearedIOConfig
    ) -> None:
        """Set the properties of the engine."""
        self.name = name
        self.deid_config = deid_config
        self.io_config = io_config
        self.results: dict[str, Any] = {}
        self._uid = f"{name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        self._validate_io_config()

    def _init_from_config(
        self, config: ClearedConfig, registry: TransformerRegistry | None = None
    ) -> None:
        """
        Initialize the engine from a configuration.

        Args:
            config: Configuration to initialize from
            registry: TransformerRegistry to use. Defaults to new registry with default transformers.

        """
        self._setup_and_validate(config.name, config.deid_config, config.io)
        self._registry = (
            registry if registry is not None else TransformerRegistry(use_defaults=True)
        )
        self._pipelines = self._load_pipelines_from_config(config)

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
        from .transformers.base import FilterableTransformer

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

        # Add filter_config if present and class supports it
        if transformer_config.filter is not None:
            if supports_filter_and_cast:
                config_dict["filter_config"] = transformer_config.filter
            else:
                raise ValueError(
                    f"Transformer {transformer_config.method} does not support filter_config but it was provided!"
                )

        # Add value_cast if present and class supports it
        if transformer_config.value_cast is not None:
            if supports_filter_and_cast:
                config_dict["value_cast"] = transformer_config.value_cast
            else:
                raise ValueError(
                    f"Transformer {transformer_config.method} does not support value_cast but it was provided!"
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
            pip = TablePipeline(table_name, config.io.data, config.deid_config)
            for transformer_config in table_config.transformers:
                # Create complete config dict including filter_config and value_cast
                config_dict = self._create_transformer_config_dict(transformer_config)

                # Create transformer with complete configs and global_deid_config
                transformer = self._registry.instantiate(
                    transformer_config.method,
                    config_dict,
                    global_deid_config=config.deid_config,
                )

                pip.add_transformer(transformer)

            pipelines.append(pip)
        return pipelines

    def run(
        self,
        continue_on_error: bool = False,
        rows_limit: int | None = None,
        test_mode: bool = False,
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

        Returns:
            Dictionary containing:
            - 'success': Boolean indicating if all pipelines completed successfully
            - 'results': Dictionary of pipeline results keyed by pipeline UID
            - 'execution_order': List of pipeline UIDs in execution order

        Raises:
            ValueError: If no pipelines are configured
            RuntimeError: If pipeline execution fails and continue_on_error is False

        """
        if self._pipelines is None or len(self._pipelines) == 0:
            raise ValueError("No pipelines configured. Add pipelines before running.")

        # Initialize de-identification reference dictionary
        current_deid_ref_dict = self._load_initial_deid_ref_dict()

        # Initialize results
        results = Results()

        # Execute each pipeline
        for table_pipeline in self._pipelines:
            current_deid_ref_dict = self._run_table_pipeline(
                table_pipeline,
                results,
                current_deid_ref_dict,
                continue_on_error,
                rows_limit=rows_limit,
                test_mode=test_mode,
            )

        # Store results in instance
        self.results = results

        # Skip saving outputs in test mode
        if not test_mode:
            self._save_results(results)
            # Save de-identification reference files
            self._save_deid_ref_files(current_deid_ref_dict)

        return results

    def _run_table_pipeline(
        self,
        table_pipeline: TablePipeline,
        results: Results,
        current_deid_ref_dict: dict[str, pd.DataFrame],
        continue_on_error: bool,
        rows_limit: int | None = None,
        test_mode: bool = False,
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

        Returns:
            Updated de-identification reference dictionary

        Raises:
            RuntimeError: If pipeline execution fails and continue_on_error is False

        """
        pipeline_uid = table_pipeline.uid
        results.add_execution_order(pipeline_uid)

        try:
            # Run the pipeline
            if hasattr(table_pipeline, "transform") and callable(
                table_pipeline.transform
            ):
                # Pipeline has transform method - execute it
                _, updated_deid_ref_dict = table_pipeline.transform(
                    df=None,
                    deid_ref_dict=current_deid_ref_dict,
                    rows_limit=rows_limit,
                    test_mode=test_mode,
                )

                # Store pipeline result
                results.add_pipeline_result(pipeline_uid, "success")
                return updated_deid_ref_dict
            else:
                results.add_pipeline_result(
                    pipeline_uid,
                    "error",
                    f"Pipeline {pipeline_uid} does not have a transform method",
                )
                return current_deid_ref_dict

        except Exception as e:
            results.add_pipeline_result(
                pipeline_uid, "error", f"Pipeline {pipeline_uid} failed: {e!s}"
            )

            # Stop execution if not continuing on error
            if not continue_on_error:
                results.set_success(False)
                raise RuntimeError(f"Pipeline execution failed: {e!s}") from e

            # If continuing on error, return the unchanged deid_ref_dict
            return current_deid_ref_dict

    def _load_initial_deid_ref_dict(self) -> dict[str, pd.DataFrame]:
        """
        Load the initial de-identification reference dictionary.

        This method reads all CSV files from the deid_ref input directory
        and loads them into a dictionary with the filename (without .csv) as the key.
        Numeric columns are automatically converted to appropriate types (int/float).

        Returns:
            Dictionary of initial de-identification reference DataFrames

        """
        deid_ref_dict = {}
        if self.io_config.deid_ref.input_config is None:
            return {}

        if self.io_config.deid_ref.input_config.io_type != "filesystem":
            return {}

        base_path = self.io_config.deid_ref.input_config.configs.get("base_path")
        if not base_path:
            return {}

        if not os.path.exists(base_path):
            raise FileNotFoundError(
                f"De-identification reference input directory {base_path} not found"
            )

        csv_pattern = os.path.join(base_path, "*.csv")
        csv_files = glob.glob(csv_pattern)

        for csv_file in csv_files:
            try:
                # Get filename without extension as the key
                filename = os.path.basename(csv_file)
                key = os.path.splitext(filename)[0]  # Remove .csv extension

                # Read CSV file
                df = pd.read_csv(csv_file)

                # Convert numeric columns to appropriate types
                df = self._convert_numeric_columns(df)

                deid_ref_dict[key] = df

            except Exception as e:
                # Log error but continue with other files
                print(f"Warning: Could not load CSV file {csv_file}: {e}")
                continue

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
        if self.io_config is None:
            raise ValueError("IO Config is required")

        if self.io_config.deid_ref is None:
            raise ValueError(
                "De-identification IO config must contain at least outout io configurations"
            )

        if self.io_config.deid_ref.input_config is not None:
            if self.io_config.deid_ref.input_config.io_type != "filesystem":
                raise ValueError(
                    "De-identification reference dictionary input configuration must be of type filesystem"
                )

        if self.io_config.deid_ref.output_config is None:
            raise ValueError(
                "De-identification reference dictionary output configuration must be provided"
            )

        if self.io_config.deid_ref.output_config.io_type != "filesystem":
            raise ValueError(
                "De-identification reference dictionary output configuration must be of type filesystem"
            )

        if self.io_config.data is None:
            raise ValueError("Data IO config must be provided")

        if self.io_config.data.input_config is None:
            raise ValueError("Data input configuration must be provided")

        if self.io_config.data.output_config is None:
            raise ValueError("Data output configuration must be provided")

    def _save_results(self, results: Results) -> None:
        """
        Save the results to the output directory.

        Args:
            results: Results to save

        """
        # Convert Results to dictionary for JSON serialization
        results_dict = {
            "success": results.success,
            "execution_order": results.execution_order,
            "results": {
                uid: {"status": result.status, "error": result.error}
                for uid, result in results.results.items()
            },
        }

        with open(
            os.path.join(self.io_config.runtime_io_path, f"status_{self._uid}.json"),
            "w",
        ) as f:
            json.dump(results_dict, f, indent=2)

    def _save_deid_ref_files(self, deid_ref_dict: dict[str, pd.DataFrame]) -> None:
        """
        Save de-identification reference files to the output directory.

        Args:
            deid_ref_dict: Dictionary of de-identification reference DataFrames

        """
        if self.io_config.deid_ref.output_config is None:
            return

        if self.io_config.deid_ref.output_config.io_type != "filesystem":
            return

        base_path = self.io_config.deid_ref.output_config.configs.get("base_path")
        if not base_path:
            return

        # Create output directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Save each reference DataFrame as CSV
        for ref_name, ref_df in deid_ref_dict.items():
            output_file = os.path.join(base_path, f"{ref_name}.csv")
            ref_df.to_csv(output_file, index=False)

    def add_pipeline(self, pipeline: Pipeline) -> None:
        """
        Add a pipeline to the engine.

        Args:
            pipeline: Pipeline instance to add

        """
        if pipeline is None:
            raise ValueError("Pipeline cannot be None")
        self._pipelines.append(pipeline)

    def remove_pipeline(self, pipeline_uid: str) -> bool:
        """
        Remove a pipeline from the engine by its UID.

        Args:
            pipeline_uid: UID of the pipeline to remove

        Returns:
            True if pipeline was found and removed, False otherwise

        """
        for i, pipeline in enumerate(self._pipelines):
            if pipeline.uid == pipeline_uid:
                del self._pipelines[i]
                return True
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
