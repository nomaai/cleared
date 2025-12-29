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

# File format to extension mapping
FILE_FORMAT_EXTENSIONS = {
    "csv": "csv",
    "parquet": "parquet",
    "json": "json",
    "xlsx": "xlsx",
    "xls": "xls",
    "pickle": "pkl",
}


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

    def _get_table_location(self, table_name: str) -> tuple[Path, bool]:
        """
        Determine if table_name refers to a file or directory.

        Args:
            table_name: Name of the table

        Returns:
            Tuple of (path, is_directory)
            - If file: (file_path, False)
            - If directory: (directory_path, True)
            - Raises TableNotFoundError if neither exists

        """
        extension = FILE_FORMAT_EXTENSIONS.get(self.file_format, self.file_format)

        # Check if it's a file first (takes precedence)
        file_path = self.base_path / f"{table_name}.{extension}"
        if file_path.exists() and file_path.is_file():
            return file_path, False

        # Check if it's a directory
        dir_path = self.base_path / table_name
        if dir_path.exists() and dir_path.is_dir():
            return dir_path, True

        # Neither exists
        raise TableNotFoundError(
            f"Table '{table_name}' not found: neither file '{file_path}' nor directory '{dir_path}' exists"
        )

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
        path, is_directory = self._get_table_location(table_name)

        if is_directory:
            # Return list of all files in directory (any format)
            files = []
            for file_path in path.iterdir():
                if file_path.is_file():
                    files.append(file_path)
            if not files:
                raise TableNotFoundError(
                    f"Table directory '{path}' exists but contains no files"
                )
            return sorted(files)
        else:
            # Return single file path
            return path

    def _get_file_path(self, table_name: str) -> Path:
        """
        Get the file path for a table.

        Args:
            table_name: Name of the table

        Returns:
            Path object for the table file

        """
        extension = FILE_FORMAT_EXTENSIONS.get(self.file_format, self.file_format)
        return self.base_path / f"{table_name}.{extension}"

    def _detect_file_format_from_path(self, file_path: Path) -> str:
        """
        Detect file format from file extension.

        Args:
            file_path: Path to the file

        Returns:
            File format string (csv, parquet, json, xlsx, xls, pickle)

        """
        extension = file_path.suffix.lower()
        extension_map = {
            ".csv": "csv",
            ".parquet": "parquet",
            ".json": "json",
            ".xlsx": "xlsx",
            ".xls": "xls",
            ".pkl": "pickle",
        }
        return extension_map.get(extension, "csv")  # Default to csv

    def _read_csv_file(
        self, file_path: Path, rows_limit: int | None = None
    ) -> pd.DataFrame:
        """Read CSV file."""
        return pd.read_csv(
            file_path,
            encoding=self.encoding,
            sep=self.separator,
            nrows=rows_limit,
        )

    def _read_parquet_file(
        self, file_path: Path, rows_limit: int | None = None
    ) -> pd.DataFrame:
        """Read Parquet file."""
        df = pd.read_parquet(file_path)
        if rows_limit is not None:
            df = df.head(rows_limit)
        return df

    def _read_json_file(
        self, file_path: Path, rows_limit: int | None = None
    ) -> pd.DataFrame:
        """Read JSON file."""
        df = pd.read_json(file_path)
        if rows_limit is not None:
            df = df.head(rows_limit)
        return df

    def _read_excel_file(
        self, file_path: Path, rows_limit: int | None = None
    ) -> pd.DataFrame:
        """Read Excel file."""
        return pd.read_excel(file_path, nrows=rows_limit)

    def _read_pickle_file(
        self, file_path: Path, rows_limit: int | None = None
    ) -> pd.DataFrame:
        """Read Pickle file."""
        df = pd.read_pickle(file_path)
        if rows_limit is not None:
            df = df.head(rows_limit)
        return df

    def _read_file_by_format(
        self, file_path: Path, file_format: str, rows_limit: int | None = None
    ) -> pd.DataFrame:
        """
        Read a file based on its format.

        Args:
            file_path: Path to the file
            file_format: Format of the file (csv, parquet, json, etc.)
            rows_limit: Optional limit on number of rows to read

        Returns:
            DataFrame containing the file data

        Raises:
            FileFormatError: If file format is unsupported or file cannot be read

        """
        try:
            if file_format == "csv":
                return self._read_csv_file(file_path, rows_limit)
            elif file_format == "parquet":
                return self._read_parquet_file(file_path, rows_limit)
            elif file_format == "json":
                return self._read_json_file(file_path, rows_limit)
            elif file_format in ["xlsx", "xls"]:
                return self._read_excel_file(file_path, rows_limit)
            elif file_format == "pickle":
                return self._read_pickle_file(file_path, rows_limit)
            else:
                raise FileFormatError(f"Unsupported file format: {file_format}")

        except FileFormatError:
            raise
        except Exception as e:
            raise FileFormatError(f"Failed to read file {file_path}: {e!s}") from e

    def read_table(
        self,
        table_name: str,
        rows_limit: int | None = None,
        segment_path: Path | None = None,
    ) -> pd.DataFrame:
        """
        Read data from a file.

        Args:
            table_name: Name of the table (file without extension)
            rows_limit: Optional limit on number of rows to read (for testing)
            segment_path: Optional path to specific segment file (for multi-segment tables)

        Returns:
            DataFrame containing the table data

        Raises:
            TableNotFoundError: If the file doesn't exist
            FileFormatError: If file cannot be read

        """
        # If segment_path is provided, read that specific file
        if segment_path is not None:
            if not segment_path.exists():
                raise TableNotFoundError(f"Segment file not found: {segment_path}")
            file_format = self._detect_file_format_from_path(segment_path)
            return self._read_file_by_format(segment_path, file_format, rows_limit)

        # Otherwise, use existing logic (backward compatible)
        file_path = self._get_file_path(table_name)

        if not file_path.exists():
            raise TableNotFoundError(f"Table file not found: {file_path}")

        return self._read_file_by_format(file_path, self.file_format, rows_limit)

    def _write_csv_file(
        self, file_path: Path, df: pd.DataFrame, index: bool = False
    ) -> None:
        """Write CSV file."""
        df.to_csv(file_path, index=index, encoding=self.encoding, sep=self.separator)

    def _write_parquet_file(
        self, file_path: Path, df: pd.DataFrame, index: bool = False
    ) -> None:
        """Write Parquet file."""
        df.to_parquet(file_path, index=index)

    def _write_json_file(
        self, file_path: Path, df: pd.DataFrame, index: bool = False
    ) -> None:
        """Write JSON file."""
        df.to_json(file_path, orient="records", index=index)

    def _write_excel_file(
        self, file_path: Path, df: pd.DataFrame, index: bool = False
    ) -> None:
        """Write Excel file."""
        df.to_excel(file_path, index=index)

    def _write_pickle_file(
        self, file_path: Path, df: pd.DataFrame, index: bool = False
    ) -> None:
        """Write Pickle file."""
        df.to_pickle(file_path)

    def _write_file_by_format(
        self,
        file_path: Path,
        df: pd.DataFrame,
        file_format: str,
        index: bool = False,
    ) -> None:
        """
        Write DataFrame to file based on format.

        Args:
            file_path: Path where file should be written
            df: DataFrame to write
            file_format: Format to write (csv, parquet, json, etc.)
            index: Whether to write DataFrame index as a column

        Raises:
            WriteError: If writing fails

        """
        try:
            # Ensure directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Write based on file format
            if file_format == "csv":
                self._write_csv_file(file_path, df, index)
            elif file_format == "parquet":
                self._write_parquet_file(file_path, df, index)
            elif file_format == "json":
                self._write_json_file(file_path, df, index)
            elif file_format in ["xlsx", "xls"]:
                self._write_excel_file(file_path, df, index)
            elif file_format == "pickle":
                self._write_pickle_file(file_path, df, index)
            else:
                raise WriteError(f"Unsupported file format: {file_format}")

        except WriteError:
            raise
        except Exception as e:
            raise WriteError(f"Failed to write file {file_path}: {e!s}") from e

    def _handle_file_exists(
        self,
        file_path: Path,
        df: pd.DataFrame,
        if_exists: str,
        file_format: str,
        table_name: str | None = None,
    ) -> pd.DataFrame:
        """
        Handle if_exists parameter logic.

        Args:
            file_path: Path to the file
            df: DataFrame to write
            if_exists: How to behave if file exists ('replace', 'append', 'fail')
            file_format: Format of the file
            table_name: Optional table name (for reading existing file)

        Returns:
            DataFrame to write (may be concatenated with existing data)

        Raises:
            WriteError: If if_exists='fail' and file exists

        """
        if not file_path.exists():
            return df

        if if_exists == "fail":
            error_msg = (
                f"Segment file already exists: {file_path}"
                if table_name is None
                else f"File already exists: {file_path}"
            )
            raise WriteError(error_msg)
        elif if_exists == "append":
            # For append, read existing file
            try:
                if table_name:
                    existing_df = self.read_table(table_name)
                else:
                    existing_df = self._read_file_by_format(file_path, file_format)
                df = pd.concat([existing_df, df], ignore_index=True)
            except (TableNotFoundError, Exception):
                pass  # File doesn't exist or can't be read, proceed with write

        return df

    def _write_segment_file(
        self,
        df: pd.DataFrame,
        table_name: str,
        segment_name: str,
        if_exists: str,
        index: bool,
    ) -> None:
        """Write segment file to table_name/segment_name directory."""
        output_dir = self.base_path / table_name
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / segment_name

        file_format = self._detect_file_format_from_path(file_path)
        df = self._handle_file_exists(file_path, df, if_exists, file_format)
        self._write_file_by_format(file_path, df, file_format, index)

    def _write_single_file(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str,
        index: bool,
    ) -> None:
        """Write single file table."""
        file_path = self._get_file_path(table_name)
        df = self._handle_file_exists(
            file_path, df, if_exists, self.file_format, table_name
        )
        self._write_file_by_format(file_path, df, self.file_format, index)

    def write_deid_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False,
        segment_name: str | None = None,
    ) -> None:
        """
        Write de-identified data to a file.

        Args:
            df: DataFrame containing the de-identified data
            table_name: Name of the table to write to
            if_exists: How to behave if file exists ('replace', 'append', 'fail')
            index: Whether to write DataFrame index as a column
            segment_name: Optional segment filename (for multi-segment tables)

        Raises:
            WriteError: If writing fails

        """
        if segment_name is not None:
            self._write_segment_file(df, table_name, segment_name, if_exists, index)
        else:
            self._write_single_file(df, table_name, if_exists, index)

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
