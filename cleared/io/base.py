"""
Base data loader classes for cleared.

This module provides abstract base classes for data loaders that can handle
both file system and SQL-based data sources for de-identification workflows.
"""

from abc import ABC, abstractmethod
from typing import Any
from pathlib import Path
import pandas as pd
from omegaconf import DictConfig


class BaseDataLoader(ABC):
    """
    Abstract base class for data loaders.

    This class defines the interface for data loaders that can read from and
    write to various data sources (file system, SQL databases, etc.) in the
    context of data de-identification workflows.

    The loader expects a Hydra configuration object that contains all necessary
    connection parameters, file paths, and other settings specific to the
    data source type.

    Attributes:
        config (DictConfig): Hydra configuration object containing loader settings
        connection_params (Dict[str, Any]): Extracted connection parameters
        data_source_type (str): Type of data source (e.g., 'filesystem', 'sql')

    Example:
        >>> from omegaconf import OmegaConf
        >>> config = OmegaConf.load("config/data_loader.yaml")
        >>> loader = ConcreteDataLoader(config)
        >>> df = loader.read_table("patients")
        >>> loader.write_deid_table(df, "patients_deid")

    """

    def __init__(self, config: DictConfig) -> None:
        """
        Initialize the data loader with Hydra configuration.

        Args:
            config: Hydra configuration object containing:
                - data_source_type: Type of data source
                - connection_params: Connection-specific parameters
                - suffix: Optional suffix for de-identified table names
                - validation_rules: Optional data validation rules

        Raises:
            ValueError: If required configuration parameters are missing

        """
        self.config = config
        self.data_source_type = self._extract_data_source_type()
        self.connection_params = self._extract_connection_params()
        self.suffix = self._extract_suffix()
        self.validation_rules = self._extract_validation_rules()
        self.table_mappings = self._extract_table_mappings()

        # Initialize the connection
        self._initialize_connection()

    def _extract_data_source_type(self) -> str:
        """
        Extract data source type from configuration.

        Returns:
            Data source type string

        Raises:
            ValueError: If data_source_type is not specified

        """
        if "data_source_type" not in self.config:
            raise ValueError("data_source_type must be specified in configuration")
        return self.config["data_source_type"]

    def _extract_connection_params(self) -> dict[str, Any]:
        """
        Extract connection parameters from configuration.

        Returns:
            Dictionary of connection parameters

        """
        return self.config.get("connection_params", {})

    def _extract_suffix(self) -> str:
        """
        Extract de-identified table name suffix from configuration.

        Returns:
            Suffix for de-identified table names

        """
        return self.config.get("suffix", "")

    def _extract_validation_rules(self) -> dict[str, Any]:
        """
        Extract data validation rules from configuration.

        Returns:
            Dictionary of validation rules

        """
        return self.config.get("validation_rules", {})

    def _extract_table_mappings(self) -> dict[str, str]:
        """
        Extract table mappings from configuration.

        Returns:
            Dictionary mapping original table names to de-identified table names

        """
        return self.config.get("table_mappings", {})

    @abstractmethod
    def _initialize_connection(self) -> None:
        """
        Initialize connection to the data source.

        This method should establish the connection to the data source
        (file system, database, etc.) based on the connection parameters.

        Raises:
            IOConnectionError: If connection cannot be established

        """
        pass

    @abstractmethod
    def get_table_paths(self, table_name: str) -> Path | list[Path]:
        """
        Get path(s) for a table.

        Args:
            table_name: Name of the table

        Returns:
            - Path: If table_name maps to a single file
            - list[Path]: If table_name maps to a directory with segment files

        Raises:
            TableNotFoundError: If table doesn't exist

        """
        pass

    @abstractmethod
    def read_table(
        self,
        table_name: str,
        rows_limit: int | None = None,
        segment_path: Path | None = None,
    ) -> pd.DataFrame:
        """
        Read data from a table.

        Args:
            table_name: Name of the table to read from
            rows_limit: Optional limit on number of rows to read (for testing)
            segment_path: Optional path to specific segment file (for multi-segment tables)

        Returns:
            DataFrame containing the table data

        Raises:
            TableNotFoundError: If the specified table doesn't exist
            IOConnectionError: If connection to data source fails
            ValidationError: If data doesn't meet validation rules

        """
        pass

    @abstractmethod
    def write_deid_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False,
        segment_name: str | None = None,
    ) -> None:
        """
        Write de-identified data to a table.

        Args:
            df: DataFrame containing the de-identified data
            table_name: Name of the table to write to
            if_exists: How to behave if table exists ('replace', 'append', 'fail')
            index: Whether to write DataFrame index as a column
            segment_name: Optional segment filename (for multi-segment tables)

        Raises:
            WriteError: If writing to data source fails
            ValidationError: If data doesn't meet validation rules

        """
        pass

    def get_deid_table_name(self, original_name: str) -> str:
        """
        Get the de-identified table name for an original table name.

        Args:
            original_name: Original table name

        Returns:
            De-identified table name

        """
        # Handle None input - convert to string representation
        if original_name is None:
            return f"None{self.suffix}"

        # Check if there's a specific mapping for this table
        if original_name in self.table_mappings:
            return self.table_mappings[original_name]

        # Fall back to suffix-based naming
        return f"{original_name}{self.suffix}"

    def get_original_table_name(self, deid_name: str) -> str:
        """
        Get the original table name for a de-identified table name.

        Args:
            deid_name: De-identified table name

        Returns:
            Original table name, or the input if no mapping found

        """
        # Handle None input
        if deid_name is None:
            return None

        # Check if this deid name maps back to an original
        for original, deid in self.table_mappings.items():
            if deid == deid_name:
                return original

        # If no mapping found, try to remove suffix
        if self.suffix and deid_name.endswith(self.suffix):
            return deid_name[: -len(self.suffix)]

        return deid_name

    def get_table_mapping(self, table_name: str) -> tuple[str, str]:
        """
        Get the mapping for a table name.

        Args:
            table_name: Table name (original or de-identified)

        Returns:
            Tuple of (original_name, deid_name)

        """
        # Handle None input - return None for original, but still generate deid name
        if table_name is None:
            return None, self.get_deid_table_name(None)

        # Check if it's an original name with a mapping
        if table_name in self.table_mappings:
            return table_name, self.table_mappings[table_name]

        # Check if it's a deid name that maps back
        original = self.get_original_table_name(table_name)
        if original != table_name:
            return original, table_name

        # Default mapping using suffix
        return table_name, self.get_deid_table_name(table_name)

    def validate_data(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        Validate data against configured rules.

        Args:
            df: DataFrame to validate
            table_name: Name of the table for context

        Returns:
            True if validation passes

        Raises:
            ValidationError: If validation fails

        """
        if not self.validation_rules:
            return True

        # Basic validation rules
        rules = self.validation_rules.get(table_name, {})

        # Check required columns
        required_columns = rules.get("required_columns", [])
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Check data types
        expected_types = rules.get("expected_types", {})
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if expected_type not in actual_type:
                    raise ValueError(
                        f"Column '{col}' has type '{actual_type}', expected '{expected_type}'"
                    )

        return True

    def list_tables(self) -> list[str]:
        """
        List available tables in the data source.

        Returns:
            List of table names

        Raises:
            IOConnectionError: If connection to data source fails

        """
        raise NotImplementedError("list_tables must be implemented by subclass")

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the data source.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists, False otherwise

        """
        try:
            tables = self.list_tables()
            return table_name in tables
        except NotImplementedError:
            # Fallback: try to read the table with rows_limit=0
            try:
                self.read_table(table_name, rows_limit=0)
                return True
            except Exception:
                return False

    def close_connection(self) -> None:
        """
        Close the connection to the data source.

        This method should clean up any resources and close connections.
        """
        return

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_connection()

    def __repr__(self) -> str:
        """Return string representation of the loader."""
        return f"{self.__class__.__name__}(data_source_type='{self.data_source_type}')"


class DataLoaderError(Exception):
    """Base exception for data loader errors."""

    pass


class IOConnectionError(DataLoaderError):
    """Raised when connection to data source fails."""

    pass


class FileFormatError(DataLoaderError):
    """Raised when data format is not supported or corrupteds."""

    pass


class TableNotFoundError(DataLoaderError):
    """Raised when a requested table doesn't exist."""

    pass


class WriteError(DataLoaderError):
    """Raised when writing to data source fails."""

    pass


class ValidationError(DataLoaderError):
    """Raised when data validation fails."""

    pass
