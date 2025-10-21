"""Base transformer class."""

from abc import ABC, abstractmethod
import pandas as pd
from uuid import uuid4
import networkx as nx


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

    def __init__(self, uid: str | None = None, dependencies: list[str] | None = None):
        """
        Initialize the base transformer.

        Args:
            uid: Unique identifier for the transformer
            dependencies: List of dependency UIDs

        """
        super().__init__(uid, dependencies)

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
    ):
        """
        Initialize the pipeline.

        Args:
            uid: Unique identifier for the pipeline (default is a random UUID)
            transformers: List of transformers to add to the pipeline (default is an empty list if None)
            dependencies: List of dependencies of the pipeline (default is an empty list if None)

        """
        super().__init__(uid, dependencies)
        self.__transformers = [] if transformers is None else transformers

    def add_transformer(self, transformer: BaseTransformer):
        """Add a transformer to the pipeline."""
        if transformer is None:
            raise ValueError("Transformer must be specified and must not be None")

        self.__transformers.append(transformer)

    def execute(self) -> None:
        """Execute the pipeline by running all transformers in sequence."""
        # This is a base implementation - subclasses should override this
        # to provide specific execution logic
        pass

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
