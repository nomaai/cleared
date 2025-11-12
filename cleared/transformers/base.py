"""Base transformer class."""

from __future__ import annotations

from abc import ABC, abstractmethod
import pandas as pd
from uuid import uuid4
import networkx as nx
from cleared.config.structure import FilterConfig, DeIDConfig


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
        # Store original index for later reconstruction
        self._original_index = df.index.copy()

        # Apply filters to get subset
        filtered_df = self.apply_filters(df)
        self._filtered_indices = filtered_df.index

        # Apply value casting if specified (after filtering, before transformation)
        if self.value_cast is not None:
            filtered_df = self._apply_value_cast(filtered_df)

        # Apply the actual transformation to the filtered subset
        transformed_df, updated_deid_ref_dict = self._apply_transform(
            filtered_df, deid_ref_dict
        )

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
            return filtered_df
        except Exception as e:
            raise ValueError(
                f"Invalid filter condition '{self.filter_config.where_condition}': {e!s}"
            ) from e

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
        except Exception as e:
            raise ValueError(
                f"Failed to cast column '{column_name}' to {self.value_cast}: {e!s}"
            ) from e

        return df

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
            raise ValueError("Transformer must be specified and must not be None")

        self.__transformers.append(transformer)

    def transform(
        self,
        df: pd.DataFrame | None = None,
        deid_ref_dict: dict[str, pd.DataFrame] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """
        Transform the data using the transformers in the pipeline.

        Args:
            df: Input DataFrame to transform
            deid_ref_dict: Dictionary of De-identification reference DataFrames, keys are the UID of the transformers that created the reference
        Returns:
            Tuple of (transformed_df, updated_deid_ref_dict)

        """
        if df is None:
            raise ValueError("DataFrame is required")

        if deid_ref_dict is None:
            raise ValueError("De-identification reference dictionary is required")

        if len(self.__transformers) == 0:
            return df, deid_ref_dict

        if self.sequential_execution:
            return self._transform_sequentially(df, deid_ref_dict)
        else:
            return self._transform_in_parallel(df, deid_ref_dict)

    def _transform_sequentially(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Transform the data using the transformers in the pipeline in sequence."""
        result_df = df
        for transformer in self.__transformers:
            result_df, deid_ref_dict = transformer.transform(result_df, deid_ref_dict)
        return result_df, deid_ref_dict

    def _transform_in_parallel(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Transform the data using the transformers in the pipeline in parallel."""
        # Build and execute DAG
        graph = nx.DiGraph()
        transformer_map = {tf.uid: tf for tf in self.__transformers}
        for tf in self.__transformers:
            graph.add_node(tf.uid)
            for dep_uid in tf.dependencies:
                graph.add_edge(dep_uid, tf.uid)

        deid_ref_dict = deid_ref_dict.copy() if deid_ref_dict is not None else None
        for tf_uid in nx.topological_sort(graph):
            df, deid_ref_dict = transformer_map[tf_uid].transform(df, deid_ref_dict)

        return df, deid_ref_dict

    @property
    def transformers(self) -> list[BaseTransformer]:
        """Get a copy of the transformers in the pipeline."""
        return tuple(self.__transformers)
