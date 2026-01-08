"""Base transformer class."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
import pandas as pd
from uuid import uuid4
import networkx as nx
import logging
from cleared.config.structure import FilterConfig, DeIDConfig
from cleared.models.verify_models import ColumnComparisonResult

# Set up logger for this module
logger = logging.getLogger(__name__)


class FormattedDataFrameError(ValueError):
    """Custom exception for formatted DataFrame errors that should not be logged."""

    pass


class BaseTask(ABC):  # noqa: B024
    """Base task class."""

    def __init__(self, uid: str | None = None, dependencies: list[str] | None = None):
        """
        Initialize the base task.

        Args:
            uid: Unique identifier for the task
            dependencies: List of dependency UIDs

        """
        self._uid = str(uuid4()) if uid is None else uid
        self._dependencies = [] if dependencies is None else dependencies

    @property
    def uid(self) -> str:
        """Get the unique identifier for this task."""
        return self._uid

    @property
    def dependencies(self) -> list[str]:
        """Get the list of dependency UIDs for this task."""
        return self._dependencies

    def add_dependency(self, uid: str):
        """
        Add a dependency to the task.

        Args:
            uid: Unique identifier of the dependency

        """
        self._dependencies.append(uid)


class BaseTransformer(BaseTask):
    """Base transformer class."""

    def __init__(
        self,
        uid: str | None = None,
        dependencies: list[str] | None = None,
        global_deid_config: DeIDConfig | None = None,
    ):
        """
        Initialize the base transformer.

        Args:
            uid: Unique identifier for the transformer
            dependencies: List of dependency UIDs
            global_deid_config: Global de-identification configuration (optional)

        """
        super().__init__(uid, dependencies)
        self.global_deid_config = global_deid_config

    @abstractmethod
    def transform(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Transform the input DataFrame and update the de-identification reference dictionary.

        Args:
            df: Input DataFrame to transform
            deid_ref_dict: Dictionary of De-identification reference DataFrames
        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        """
        pass

    @abstractmethod
    def reverse(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Reverse the transformation to restore original values.

        By default, this calls transform() which should be overridden by subclasses
        that need custom reverse behavior.

        Args:
            df: Input DataFrame to reverse
            deid_ref_dict: Dictionary of De-identification reference DataFrames
        Returns:
            Tuple of (reversed_df, updated_deid_ref_dict)

        """
        pass

    @abstractmethod
    def compare(
        self,
        original_df: pd.DataFrame,
        reversed_df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
    ) -> list[ColumnComparisonResult]:
        """
        Compare original and reversed DataFrames to verify correctness.

        Args:
            original_df: Original DataFrame before transformation
            reversed_df: Reversed DataFrame after reverse transformation
            deid_ref_dict: Dictionary of de-identification reference DataFrames (optional)

        Returns:
            List of ColumnComparisonResult objects

        """
        pass


class FilterableTransformer(BaseTransformer):
    """Filterable transformer class that applies filters before transformation."""

    def __init__(
        self,
        filter_config: FilterConfig | None = None,
        value_cast: str | None = None,
        uid: str | None = None,
        dependencies: list[str] | None = None,
        global_deid_config: DeIDConfig | None = None,
    ):
        """
        Initialize the filterable transformer.

        Args:
            filter_config: Configuration for filtering operations
            value_cast: Type to cast the de-identification column to ("integer", "float", "string", "datetime")
            uid: Unique identifier for the transformer
            dependencies: List of dependency UIDs
            global_deid_config: Global de-identification configuration (optional)

        """
        super().__init__(uid, dependencies, global_deid_config)
        self.filter_config = filter_config
        self.value_cast = value_cast
        self._original_index = None
        self._filtered_indices = None

    def transform(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Transform the input DataFrame and update the de-identification reference dictionary.

        Args:
            df: Input DataFrame to transform
            deid_ref_dict: Dictionary of De-identification reference DataFrames
        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        """
        return self.filter_and_apply(df, deid_ref_dict, self._apply_transform)

    @abstractmethod
    def _apply_transform(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Apply the actual transformation to the filtered DataFrame.

        Args:
            df: Filtered DataFrame to transform
            deid_ref_dict: Dictionary of De-identification reference DataFrames
        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        """
        pass

    def reverse(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Reverse the transformation to restore original values.

        Args:
            df: Input DataFrame to reverse
            deid_ref_dict: Dictionary of De-identification reference DataFrames
        Returns:
            Tuple of (reversed_df, updated_deid_ref_dict)

        """
        return self.filter_and_apply(df, deid_ref_dict, self._apply_reverse)

    @abstractmethod
    def _apply_reverse(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Apply the actual reverse transformation to the filtered DataFrame.

        By default, this calls _apply_transform. Subclasses should override
        this method if they need custom reverse behavior.

        Args:
            df: Filtered DataFrame to reverse
            deid_ref_dict: Dictionary of De-identification reference DataFrames
        Returns:
            Tuple of (reversed_df, updated_deid_ref_dict)

        """
        pass

    def compare(
        self,
        original_df: pd.DataFrame,
        reversed_df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
    ) -> list[ColumnComparisonResult]:
        """
        Compare original and reversed DataFrames with filtering and casting applied.

        This method applies the same filters and value casting as transform/reverse,
        then calls the abstract _compare method for the actual comparison.

        Args:
            original_df: Original DataFrame before transformation
            reversed_df: Reversed DataFrame after reverse transformation
            deid_ref_dict: Dictionary of de-identification reference DataFrames (optional)

        Returns:
            List of ColumnComparisonResult objects

        """
        if deid_ref_dict is None:
            deid_ref_dict = {}

        # Apply filters to both DataFrames
        original_filtered = self.apply_filters(original_df)
        reversed_filtered = self.apply_filters(reversed_df)

        # Apply value casting if specified
        if self.value_cast is not None:
            original_filtered = self._apply_value_cast(original_filtered)
            reversed_filtered = self._apply_value_cast(reversed_filtered)

        # Call the abstract _compare method
        return self._compare(original_filtered, reversed_filtered, deid_ref_dict)

    @abstractmethod
    def _compare(
        self,
        original_df: pd.DataFrame,
        reversed_df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
    ) -> list[ColumnComparisonResult]:
        """
        Compare filtered and cast original and reversed DataFrames.

        Subclasses should implement this method to perform the actual comparison
        logic for their specific transformation type.

        Args:
            original_df: Filtered and cast original DataFrame
            reversed_df: Filtered and cast reversed DataFrame
            deid_ref_dict: Dictionary of de-identification reference DataFrames

        Returns:
            List of ColumnComparisonResult objects

        """
        pass

    def filter_and_apply(
        self,
        df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
        apply_fnc: Callable[
            [pd.DataFrame, dict[str, pd.DataFrame]],
            tuple[pd.DataFrame, dict[str, pd.DataFrame]],
        ],
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Filter the input DataFrame and apply a function to the filtered DataFrame."""
        # Store original index for later reconstruction
        self._original_index = df.index.copy()

        # Apply filters to get subset
        filtered_df = self.apply_filters(df)
        self._filtered_indices = filtered_df.index

        # Apply value casting if specified (after filtering, before transformation)
        if self.value_cast is not None:
            filtered_df = self._apply_value_cast(filtered_df)

        # Apply the actual transformation to the filtered subset
        transformed_df, updated_deid_ref_dict = apply_fnc(filtered_df, deid_ref_dict)

        # Ensure transformed_df has the same index as filtered_df
        transformed_df = transformed_df.set_index(filtered_df.index)

        # Reconstruct the full DataFrame with original row order
        result_df = self.undo_filters(df, transformed_df)

        return result_df, updated_deid_ref_dict

    def apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the filters to the input DataFrame using SQL-like WHERE conditions.

        Args:
            df: Input DataFrame to filter
        Returns:
            Filtered DataFrame containing only rows that match the filter condition

        """
        if self.filter_config is None:
            return df

        # Use pandas query method to apply SQL-like WHERE conditions
        # Pandas will raise exceptions (SyntaxError, UndefinedVariableError, etc.) for invalid conditions
        # We wrap them as ValueError for consistent API and to include the filter condition in the error message
        try:
            filtered_df = df.query(self.filter_config.where_condition)
            filtered_count = len(filtered_df)
            original_count = len(df)
            logger.debug(
                f"Transformer {self.uid} filtered {original_count} rows to {filtered_count} rows using condition: {self.filter_config.where_condition}"
            )
            return filtered_df
        except Exception as e:
            error_msg = f"Invalid filter condition '{self.filter_config.where_condition}': {e!s}"
            logger.error(f"Transformer {self.uid} filter failed: {error_msg}")
            raise RuntimeError(error_msg) from e

    def undo_filters(
        self, original_df: pd.DataFrame, transformed_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Reconstruct the full DataFrame by merging transformed rows back into original positions.

        Args:
            original_df: Original DataFrame before filtering
            transformed_df: DataFrame after transformation of filtered subset
        Returns:
            Full DataFrame with transformed rows in their original positions

        """
        if self.filter_config is None or self._filtered_indices is None:
            return transformed_df

        # Create a copy of the original DataFrame
        result_df = original_df.copy()

        # Use vectorized operations to update the filtered rows
        # Assumes columns between original and transformed are the same
        result_df.loc[self._filtered_indices] = transformed_df.loc[
            self._filtered_indices
        ]
        return result_df

    def _get_column_to_cast(self) -> str | None:
        """
        Get the name of the column that should be cast.

        Subclasses should override this method to return the column name
        that should be cast when value_cast is specified.

        Returns:
            Column name to cast, or None if not applicable

        """
        return None

    def _apply_value_cast(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply value casting to the de-identification column if value_cast is specified.

        Args:
            df: DataFrame to cast values in

        Returns:
            DataFrame with cast values

        """
        if self.value_cast is None:
            return df

        column_name = self._get_column_to_cast()
        if column_name is None or column_name not in df.columns:
            return df

        df = df.copy()

        try:
            if self.value_cast == "integer":
                # Convert to integer, handling strings that represent integers
                numeric_series = pd.to_numeric(df[column_name], errors="coerce")
                # Use nullable integer type if there are any NaN values, otherwise int64
                if numeric_series.isna().any():
                    df[column_name] = numeric_series.astype("Int64")
                else:
                    df[column_name] = numeric_series.astype("int64")
            elif self.value_cast == "float":
                # Convert to float
                df[column_name] = pd.to_numeric(
                    df[column_name], errors="coerce"
                ).astype("float64")
            elif self.value_cast == "string":
                # Convert to string
                df[column_name] = df[column_name].astype(str)
            elif self.value_cast == "datetime":
                # Convert to datetime, handling various input formats
                df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
            logger.debug(
                f"Transformer {self.uid} cast column '{column_name}' to {self.value_cast}"
            )
        except Exception as e:
            error_msg = (
                f"Failed to cast column '{column_name}' to {self.value_cast}: {e!s}"
            )
            logger.error(f"Transformer {self.uid} value cast failed: {error_msg}")
            raise ValueError(error_msg) from e

        return df


# ============================================================
# PIPELINE CLASS (ALSO A TRANSFORMER)
# ============================================================


class Pipeline(BaseTransformer):
    """Pipeline class for chaining transformers."""

    def __init__(
        self,
        uid: str | None = None,
        transformers: list[BaseTransformer] | None = None,
        dependencies: list[str] | None = None,
        sequential_execution: bool = True,
        global_deid_config: DeIDConfig | None = None,
    ):
        """
        Initialize the pipeline.

        Args:
            uid: Unique identifier for the pipeline (default is a random UUID)
            transformers: List of transformers to add to the pipeline (default is an empty list if None)
            dependencies: List of dependencies of the pipeline (default is an empty list if None)
            sequential_execution: Whether to execute the transformers in sequence (default is True)
            global_deid_config: Global de-identification configuration (optional)

        """
        super().__init__(uid, dependencies, global_deid_config)
        self.__transformers = [] if transformers is None else transformers
        self.sequential_execution = sequential_execution

    def add_transformer(self, transformer: BaseTransformer):
        """Add a transformer to the pipeline."""
        if transformer is None:
            logger.error(f"Pipeline {self.uid} attempted to add None transformer")
            raise ValueError("Transformer must be specified and must not be None")

        self.__transformers.append(transformer)
        logger.debug(
            f"Pipeline {self.uid} added transformer: {transformer.uid} (total: {len(self.__transformers)})"
        )

    def transform(
        self,
        df: pd.DataFrame | None = None,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
        test_mode: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Transform the data using the transformers in the pipeline.

        Args:
            df: Input DataFrame to transform
            deid_ref_dict: Dictionary of De-identification reference DataFrames, keys are the UID of the transformers that created the reference
            test_mode: If True, suppress transformer logging (for cleaner test output)

        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        """
        self._validate_input(df, deid_ref_dict)
        if len(self.__transformers) == 0:
            return df, deid_ref_dict

        if self.sequential_execution:
            return self._run_sequentially(
                df, deid_ref_dict, reverse=False, test_mode=test_mode
            )
        else:
            return self._transform_in_parallel(df, deid_ref_dict, test_mode=test_mode)

    def reverse(
        self,
        df: pd.DataFrame | None = None,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
        test_mode: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Reverse the transformations using the transformers in the pipeline.

        Args:
            df: Input DataFrame to reverse
            deid_ref_dict: Dictionary of De-identification reference DataFrames, keys are the UID of the transformers that created the reference
            test_mode: If True, suppress transformer logging (for cleaner test output)

        Returns:
            Tuple of (reversed_df, updated_deid_ref_dict)

        """
        self._validate_input(df, deid_ref_dict)
        if len(self.__transformers) == 0:
            return df, deid_ref_dict

        if self.sequential_execution:
            return self._run_sequentially(
                df, deid_ref_dict, reverse=True, test_mode=test_mode
            )
        else:
            return self._reverse_in_parallel(df, deid_ref_dict, test_mode=test_mode)

    def compare(
        self,
        original_df: pd.DataFrame | None = None,
        reversed_df: pd.DataFrame | None = None,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
    ) -> list[ColumnComparisonResult]:
        """
        Compare original and reversed DataFrames using all transformers in the pipeline.

        Args:
            original_df: Original DataFrame before transformation
            reversed_df: Reversed DataFrame after reverse transformation
            deid_ref_dict: Dictionary of de-identification reference DataFrames (optional)

        Returns:
            List of ColumnComparisonResult objects, one per transformer

        """
        if original_df is None or reversed_df is None:
            return [
                ColumnComparisonResult(
                    column_name="pipeline_error",
                    status="error",
                    message="Both original_df and reversed_df must be provided",
                    original_length=0,
                    reversed_length=0,
                    mismatch_count=0,
                    mismatch_percentage=0.0,
                )
            ]

        if deid_ref_dict is None:
            deid_ref_dict = {}

        if len(self.__transformers) == 0:
            return [
                ColumnComparisonResult(
                    column_name="pipeline_empty",
                    status="pass",
                    message="No transformers in pipeline to compare",
                    original_length=len(original_df),
                    reversed_length=len(reversed_df),
                    mismatch_count=0,
                    mismatch_percentage=0.0,
                )
            ]

        # Collect results from all transformers
        results: list[ColumnComparisonResult] = []

        for transformer in self.__transformers:
            try:
                result = transformer.compare(original_df, reversed_df, deid_ref_dict)
                # Update column_name to include transformer UID for clarity
                for r in result:
                    r.column_name = f"{transformer.uid}_{r.column_name}"
                    results.append(r)
            except Exception as e:
                results.append(
                    ColumnComparisonResult(
                        column_name=f"{transformer.uid}_error",
                        status="error",
                        message=f"Error during comparison: {e!s}",
                        original_length=len(original_df),
                        reversed_length=len(reversed_df),
                        mismatch_count=0,
                        mismatch_percentage=0.0,
                    )
                )

        return results

    def _format_transformer_error(
        self,
        transformer: BaseTransformer,
        operation: str,
        error: Exception,
        df: pd.DataFrame | None = None,
        table_name: str | None = None,
    ) -> str:
        """
        Format a transformer error message with context.

        Args:
            transformer: The transformer that failed
            operation: The operation being performed (transform/reverse)
            error: The exception that occurred
            df: Optional DataFrame to show available columns
            table_name: Optional table name for additional context

        Returns:
            Formatted error message with colors and indentation

        """
        # ANSI color codes
        RED = "\033[91m"
        RESET = "\033[0m"
        BOLD = "\033[1m"
        DIM = "\033[2m"

        # Extract column name from common error messages
        error_str = str(error)
        if "not found in DataFrame" in error_str or "not found" in error_str:
            # Try to extract column name from error message
            import re

            match = re.search(r"['\"]([^'\"]+)['\"]", error_str)
            if match:
                column_name = match.group(1)

                # Build formatted error message with indentation
                lines = []
                lines.append(f"{BOLD}{RED}✗ Missing Column Error{RESET}")
                lines.append("")

                if table_name:
                    lines.append(
                        f"  {DIM}Table:{RESET}        {RED}{BOLD}{table_name}{RESET}"
                    )
                else:
                    lines.append(
                        f"  {DIM}Table:{RESET}        {RED}{BOLD}(unknown){RESET}"
                    )

                lines.append(
                    f"  {DIM}Missing Column:{RESET} {RED}{BOLD}{column_name}{RESET}"
                )
                lines.append("")

                if df is not None and hasattr(df, "columns"):
                    available_columns = sorted(df.columns.tolist())
                    lines.append(
                        f"  {DIM}Available columns ({len(available_columns)}):{RESET}"
                    )
                    # Format columns in a readable way (max 4 per line, indented)
                    for i in range(0, len(available_columns), 4):
                        cols = available_columns[i : i + 4]
                        # Pad column names for alignment
                        padded_cols = [f"{col:<25}" for col in cols]
                        lines.append(f"    {' '.join(padded_cols).rstrip()}")
                else:
                    lines.append(f"  {DIM}Available columns:{RESET} (not available)")

                return "\n".join(lines)

        # Fallback to original error message
        return f"{error!s}"

    def _run_sequentially(
        self,
        df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
        reverse: bool = False,
        test_mode: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Run the transformers in the pipeline in sequence."""
        mode = "reverse" if reverse else "transform"
        if not test_mode:
            logger.info(
                f"    Running {len(self.__transformers)} transformer(s) sequentially in {mode} mode"
            )
        result_df = df
        transformers = (
            self.__transformers if not reverse else reversed(self.__transformers)
        )
        for idx, transformer in enumerate(transformers):
            if not test_mode:
                logger.info(
                    f"      → Transformer {idx + 1}/{len(self.__transformers)}: {transformer.uid}"
                )
            try:
                if not reverse:
                    result_df, deid_ref_dict = transformer.transform(
                        result_df, deid_ref_dict
                    )
                else:
                    result_df, deid_ref_dict = transformer.reverse(
                        result_df, deid_ref_dict
                    )
            except (ValueError, KeyError, AttributeError, RuntimeError) as e:
                # Check if it's a filter condition error first (should be RuntimeError)
                error_str = str(e).lower()
                if "invalid filter condition" in error_str:
                    # Filter errors should be RuntimeError - re-raise as-is
                    raise
                # Check if it's a DataFrame-related error
                if any(
                    keyword in error_str
                    for keyword in ["column", "not found", "dataframe", "index", "key"]
                ):
                    # Get table name if this is a TablePipeline
                    table_name = getattr(self, "table_name", None)
                    formatted_error = self._format_transformer_error(
                        transformer, mode, e, result_df, table_name
                    )
                    # Re-raise with better context (don't log here - let higher level handle it)
                    raise FormattedDataFrameError(formatted_error) from e
                # Re-raise other errors as-is
                raise
        logger.debug("    Completed sequential execution")
        return result_df, deid_ref_dict

    def _validate_input(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
        """Validate the input data and de-identification reference dictionary."""
        if df is None:
            logger.error(f"Pipeline {self.uid} received None DataFrame")
            raise ValueError("DataFrame is required")

        if deid_ref_dict is None:
            logger.error(f"Pipeline {self.uid} received None deid_ref_dict")
            raise ValueError("De-identification reference dictionary is required")

    def _reverse_in_parallel(
        self,
        df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
        test_mode: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Reverse the data using the transformers in the pipeline in parallel (reverse topological order)."""
        return self._run_in_parallel(
            df, deid_ref_dict, reverse=True, test_mode=test_mode
        )

    def _transform_in_parallel(
        self,
        df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
        test_mode: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Transform the data using the transformers in the pipeline in parallel."""
        return self._run_in_parallel(
            df, deid_ref_dict, reverse=False, test_mode=test_mode
        )

    def _run_in_parallel(
        self,
        df: pd.DataFrame,
        deid_ref_dict: dict[str, pd.DataFrame],
        reverse: bool = False,
        test_mode: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Run the transformers in the pipeline in parallel."""
        mode = "reverse" if reverse else "transform"
        if not test_mode:
            logger.info(
                f"    Running {len(self.__transformers)} transformer(s) in parallel ({mode} mode)"
            )
        # Build and execute DAG
        graph = nx.DiGraph()
        transformer_map = {tf.uid: tf for tf in self.__transformers}
        for tf in self.__transformers:
            graph.add_node(tf.uid)
            for dep_uid in tf.dependencies:
                graph.add_edge(dep_uid, tf.uid)

        deid_ref_dict = deid_ref_dict.copy() if deid_ref_dict is not None else None
        ordered_transformers = [
            transformer_map[tf_uid] for tf_uid in nx.topological_sort(graph)
        ]
        if reverse:
            ordered_transformers = reversed(ordered_transformers)

        logger.debug(f"    Execution order: {[tf.uid for tf in ordered_transformers]}")
        for idx, transformer in enumerate(ordered_transformers):
            if not test_mode:
                logger.info(
                    f"      → Transformer {idx + 1}/{len(ordered_transformers)}: {transformer.uid}"
                )
            try:
                if not reverse:
                    df, deid_ref_dict = transformer.transform(df, deid_ref_dict)
                else:
                    df, deid_ref_dict = transformer.reverse(df, deid_ref_dict)
            except (ValueError, KeyError, AttributeError, RuntimeError) as e:
                # Check if it's a filter condition error first (should be RuntimeError)
                error_str = str(e).lower()
                if "invalid filter condition" in error_str:
                    # Filter errors should be RuntimeError - re-raise as-is
                    raise
                # Check if it's a DataFrame-related error
                if any(
                    keyword in error_str
                    for keyword in ["column", "not found", "dataframe", "index", "key"]
                ):
                    # Get table name if this is a TablePipeline
                    table_name = getattr(self, "table_name", None)
                    formatted_error = self._format_transformer_error(
                        transformer, mode, e, df, table_name
                    )
                    # Re-raise with better context (don't log here - let higher level handle it)
                    raise FormattedDataFrameError(formatted_error) from e
                # Re-raise other errors as-is
                raise
        logger.debug(f"Pipeline {self.uid} completed parallel execution")
        return df, deid_ref_dict

    @property
    def transformers(self) -> list[BaseTransformer]:
        """Get a copy of the transformers in the pipeline."""
        return tuple(self.__transformers)
