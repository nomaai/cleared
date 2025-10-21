"""Tests for data loaders."""

import pytest
import pandas as pd
import tempfile
import os
from omegaconf import DictConfig

from cleared.io import BaseDataLoader, FileSystemDataLoader
from cleared.io.base import (
    DataLoaderError,
    IOConnectionError,
    TableNotFoundError,
    WriteError,
    ValidationError,
)


class TestBaseDataLoader:
    """Test the abstract base class."""

    def test_base_class_is_abstract(self):
        """Test that BaseDataLoader cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseDataLoader(DictConfig({"data_source_type": "test"}))


class TestFileSystemDataLoader:
    """Test the file system data loader."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {"base_path": self.temp_dir, "file_format": "csv"},
            }
        )

        # Create test data
        self.test_df = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
        )
        self.test_df.to_csv(os.path.join(self.temp_dir, "test_table.csv"), index=False)

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_initialization(self):
        """Test loader initialization."""
        loader = FileSystemDataLoader(self.config)
        assert loader.data_source_type == "filesystem"
        assert str(loader.base_path) == self.temp_dir
        assert loader.file_format == "csv"
        loader.close_connection()

    def test_read_table_success(self):
        """Test successful table reading."""
        with FileSystemDataLoader(self.config) as loader:
            df = loader.read_table("test_table")
            assert len(df) == 3
            assert list(df.columns) == ["id", "name", "age"]
            assert df.iloc[0]["name"] == "Alice"

    def test_read_table_not_found(self):
        """Test reading non-existent table."""
        with FileSystemDataLoader(self.config) as loader:
            with pytest.raises(TableNotFoundError):
                loader.read_table("nonexistent_table")

    def test_write_deid_table(self):
        """Test writing de-identified table."""
        with FileSystemDataLoader(self.config) as loader:
            # Write de-identified data
            deid_df = self.test_df.copy()
            deid_df["id"] = deid_df["id"] + 1000
            loader.write_deid_table(deid_df, "test_table_deid")

            # Verify it was written
            assert "test_table_deid.csv" in os.listdir(self.temp_dir)

            # Read it back
            df = loader.read_table("test_table_deid")
            assert len(df) == 3
            assert df.iloc[0]["id"] == 1001

    def test_list_tables(self):
        """Test listing available tables."""
        with FileSystemDataLoader(self.config) as loader:
            tables = loader.list_tables()
            assert "test_table" in tables

    def test_table_exists(self):
        """Test checking if table exists."""
        with FileSystemDataLoader(self.config) as loader:
            assert loader.table_exists("test_table")
            assert not loader.table_exists("nonexistent_table")

    def test_context_manager(self):
        """Test context manager functionality."""
        with FileSystemDataLoader(self.config) as loader:
            assert loader is not None
        # Should not raise any errors when exiting context


class TestCustomDataLoader:
    """Test a custom data loader implementation."""

    def test_custom_loader(self):
        """Test custom loader implementation."""

        class CustomLoader(BaseDataLoader):
            def _initialize_connection(self):
                self.data = {}

            def read_table(self, table_name):
                if table_name not in self.data:
                    raise TableNotFoundError(f"Table {table_name} not found")
                return self.data[table_name]

            def write_deid_table(
                self, df, table_name, if_exists="replace", index=False
            ):
                self.data[table_name] = df.copy()

        config = DictConfig({"data_source_type": "custom", "connection_params": {}})

        with CustomLoader(config) as loader:
            # Test writing
            test_df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
            loader.write_deid_table(test_df, "test")

            # Test reading
            df = loader.read_table("test")
            assert len(df) == 2
            assert list(df.columns) == ["id", "name"]

            # Test error handling
            with pytest.raises(TableNotFoundError):
                loader.read_table("nonexistent")


class TestExceptions:
    """Test custom exceptions."""

    def test_exception_hierarchy(self):
        """Test that custom exceptions inherit from base exception."""
        assert issubclass(IOConnectionError, DataLoaderError)
        assert issubclass(TableNotFoundError, DataLoaderError)
        assert issubclass(WriteError, DataLoaderError)
        assert issubclass(ValidationError, DataLoaderError)

    def test_exception_instantiation(self):
        """Test that exceptions can be instantiated."""
        conn_error = ConnectionError("Connection failed")
        assert str(conn_error) == "Connection failed"

        table_error = TableNotFoundError("Table not found")
        assert str(table_error) == "Table not found"
