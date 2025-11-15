"""
File system based data loader implementation.

This module provides a concrete implementation of BaseDataLoader for
file system based data sources (CSV, Parquet, JSON, etc.).
"""

from pathlib import Path
import pandas as pd

from .base import (
    BaseDataLoader,
    IOConnectionError,
    TableNotFoundError,
    WriteError,
    FileFormatError,
)


class FileSystemDataLoader(BaseDataLoader):
    """
    Data loader for file system based data sources.

    This loader handles reading from and writing to various file formats
    stored on the local file system or network file systems.

    Supported formats:
        - CSV (.csv)
        - Parquet (.parquet)
        - JSON (.json)
        - Excel (.xlsx, .xls)
        - Pickle (.pkl)

    Configuration example:
        data_source_type: filesystem
        connection_params:
            base_path: "/path/to/data"
            file_format: "csv"  # csv, parquet, json, excel, pickle
            encoding: "utf-8"
            separator: ","
        table_mappings:
            patients: patients_deid
            encounters: encounters_deid
        validation_rules:
            patients:
                required_columns: ["patient_id", "age", "gender"]
                expected_types:
                    patient_id: "int64"
                    age: "int64"
    """

    def _initialize_connection(self) -> None:
        """
        Initialize file system connection.

        Raises:
            IOConnectionError: If base path doesn't exist or is not accessible

        """
        self.base_path = Path(self.connection_params.get("base_path", "."))
        self.file_format = self.connection_params.get("file_format", "csv")
        self.encoding = self.connection_params.get("encoding") or "utf-8"
        self.separator = self.connection_params.get("separator") or ","

        # Create base path if it doesn't exist
        if not self.base_path.exists():
            try:
                self.base_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise IOConnectionError(
                    f"Failed to create base path {self.base_path}: {e!s}"
                ) from e

        if not self.base_path.is_dir():
            raise IOConnectionError(f"Base path is not a directory: {self.base_path}")

    def _get_file_path(self, table_name: str) -> Path:
        """
        Get the file path for a table.

        Args:
            table_name: Name of the table

        Returns:
            Path object for the table file

        """
        # Map file formats to their extensions
        extensions = {
            "csv": "csv",
            "parquet": "parquet",
            "json": "json",
            "xlsx": "xlsx",
            "xls": "xls",
            "pickle": "pkl",
        }
        extension = extensions.get(self.file_format, self.file_format)
        return self.base_path / f"{table_name}.{extension}"

    def read_table(
        self, table_name: str, rows_limit: int | None = None
    ) -> pd.DataFrame:
        """
        Read data from a file.

        Args:
            table_name: Name of the table (file without extension)
            rows_limit: Optional limit on number of rows to read (for testing)

        Returns:
            DataFrame containing the table data

        Raises:
            TableNotFoundError: If the file doesn't exist
            DataCorruptedError: If file cannot be read

        """
        file_path = self._get_file_path(table_name)

        if not file_path.exists():
            raise TableNotFoundError(f"Table file not found: {file_path}")

        try:
            # Read based on file format
            if self.file_format == "csv":
                df = pd.read_csv(
                    file_path,
                    encoding=self.encoding,
                    sep=self.separator,
                    nrows=rows_limit,
                )
            elif self.file_format == "parquet":
                df = pd.read_parquet(file_path)
                if rows_limit is not None:
                    df = df.head(rows_limit)
            elif self.file_format == "json":
                df = pd.read_json(file_path)
                if rows_limit is not None:
                    df = df.head(rows_limit)
            elif self.file_format in ["xlsx", "xls"]:
                df = pd.read_excel(file_path, nrows=rows_limit)
            elif self.file_format == "pickle":
                df = pd.read_pickle(file_path)
                if rows_limit is not None:
                    df = df.head(rows_limit)
            else:
                raise FileFormatError(f"Unsupported file format: {self.file_format}")

            return df

        except Exception as e:
            raise FileFormatError(f"Failed to read table {table_name}: {e!s}") from e

    def write_deid_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False,
    ) -> None:
        """
        Write de-identified data to a file.

        Args:
            df: DataFrame containing the de-identified data
            table_name: Name of the table to write to
            if_exists: How to behave if file exists ('replace', 'append', 'fail')
            index: Whether to write DataFrame index as a column

        Raises:
            WriteError: If writing fails

        """
        file_path = self._get_file_path(table_name)

        # Handle if_exists parameter
        if file_path.exists():
            if if_exists == "fail":
                raise WriteError(f"File already exists: {file_path}")
            elif if_exists == "append":
                # For append, we need to read existing data first
                try:
                    existing_df = self.read_table(table_name)
                    df = pd.concat([existing_df, df], ignore_index=True)
                except TableNotFoundError:
                    pass  # File doesn't exist, proceed with write

        try:
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Write based on file format
            if self.file_format == "csv":
                df.to_csv(
                    file_path, index=index, encoding=self.encoding, sep=self.separator
                )
            elif self.file_format == "parquet":
                df.to_parquet(file_path, index=index)
            elif self.file_format == "json":
                df.to_json(file_path, orient="records", index=index)
            elif self.file_format in ["xlsx", "xls"]:
                df.to_excel(file_path, index=index)
            elif self.file_format == "pickle":
                df.to_pickle(file_path)
            else:
                raise WriteError(f"Unsupported file format: {self.file_format}")

        except Exception as e:
            raise WriteError(f"Failed to write table {table_name}: {e!s}") from e

    def list_tables(self) -> list[str]:
        """
        List available tables (files) in the data source.

        Returns:
            List of table names (without extensions)

        """
        tables = []
        for file_path in self.base_path.glob(f"*.{self.file_format}"):
            tables.append(file_path.stem)
        return sorted(tables)

    def list_original_tables(self) -> list[str]:
        """
        List original table names (before any mapping).

        Returns:
            List of original table names

        """
        all_tables = self.list_tables()
        original_tables = []

        for table in all_tables:
            # Check if this is a mapped deid table
            original = self.get_original_table_name(table)
            if original not in original_tables:
                original_tables.append(original)

        return sorted(original_tables)

    def list_deid_tables(self) -> list[str]:
        """
        List de-identified table names.

        Returns:
            List of de-identified table names

        """
        all_tables = self.list_tables()
        deid_tables = []

        for table in all_tables:
            # Check if this is a deid table (either mapped or suffixed)
            if table in self.table_mappings.values() or (
                self.suffix and table.endswith(self.suffix)
            ):
                deid_tables.append(table)

        return sorted(deid_tables)

    def close_connection(self) -> None:
        """Close file system connection (no-op for file system)."""
        pass
