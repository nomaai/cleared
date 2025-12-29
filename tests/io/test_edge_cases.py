"""Edge cases and error scenario tests for IO classes."""

import pytest
import pandas as pd
import tempfile
from pathlib import Path
from omegaconf import DictConfig

from cleared.io.base import (
    BaseDataLoader,
    IOConnectionError,
    TableNotFoundError,
    WriteError,
    FileFormatError,
)
from cleared.io.filesystem import FileSystemDataLoader


def _has_parquet_support():
    """Check if parquet support is available."""
    import importlib.util

    return (
        importlib.util.find_spec("pyarrow") is not None
        or importlib.util.find_spec("fastparquet") is not None
    )


# Check if SQLAlchemy is available
try:
    from cleared.io.sql import SQLDataLoader

    SQL_AVAILABLE = True
except ImportError:
    SQL_AVAILABLE = False
    SQLDataLoader = None


class TestEdgeCases:
    """Test edge cases and error scenarios for IO classes."""

    def test_base_loader_abstract_methods(self):
        """Test that BaseDataLoader abstract methods raise NotImplementedError."""

        # Create a concrete implementation that doesn't implement abstract methods
        class IncompleteDataLoader(BaseDataLoader):
            def _initialize_connection(self):
                pass

            def get_table_paths(self, table_name: str):
                raise NotImplementedError("get_table_paths not implemented")

            def read_table(self, table_name: str, rows_limit=None, segment_path=None):
                raise NotImplementedError("read_table not implemented")

            def write_deid_table(
                self,
                df,
                table_name,
                if_exists="replace",
                index=False,
                segment_name=None,
            ):
                raise NotImplementedError("write_deid_table not implemented")

        config = DictConfig({"data_source_type": "test"})
        loader = IncompleteDataLoader(config)

        # Should raise NotImplementedError for unimplemented abstract methods
        with pytest.raises(NotImplementedError):
            loader.get_table_paths("test")

        with pytest.raises(NotImplementedError):
            loader.read_table("test")

        with pytest.raises(NotImplementedError):
            loader.write_deid_table(pd.DataFrame(), "test")

    def test_filesystem_empty_dataframe(self):
        """Test FileSystemDataLoader with empty DataFrame."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {"base_path": temp_dir, "file_format": "csv"},
                }
            )

            loader = FileSystemDataLoader(config)

            # Test writing empty DataFrame with at least one column
            empty_df = pd.DataFrame(columns=["col1"])
            loader.write_deid_table(empty_df, "empty_table")

            # Verify file was created
            test_file = Path(temp_dir) / "empty_table.csv"
            assert test_file.exists()

            # Read it back
            read_df = loader.read_table("empty_table")
            assert len(read_df) == 0
            assert list(read_df.columns) == ["col1"]

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.skipif(
        not _has_parquet_support(),
        reason="Parquet support requires pyarrow or fastparquet",
    )
    def test_filesystem_large_dataframe(self):
        """Test FileSystemDataLoader with large DataFrame."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {
                        "base_path": temp_dir,
                        "file_format": "parquet",  # Use parquet for better performance
                    },
                }
            )

            loader = FileSystemDataLoader(config)

            # Create large DataFrame
            large_df = pd.DataFrame(
                {
                    "id": range(10000),
                    "name": [f"Person_{i}" for i in range(10000)],
                    "value": [i * 0.1 for i in range(10000)],
                }
            )

            # Write large DataFrame
            loader.write_deid_table(large_df, "large_table")

            # Read it back
            read_df = loader.read_table("large_table")
            pd.testing.assert_frame_equal(read_df, large_df)

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_filesystem_special_characters_in_data(self):
        """Test FileSystemDataLoader with special characters in data."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {
                        "base_path": temp_dir,
                        "file_format": "csv",
                        "encoding": "utf-8",
                    },
                }
            )

            loader = FileSystemDataLoader(config)

            # Create DataFrame with special characters
            special_df = pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["José", "François", "北京"],
                    "description": [
                        "Special chars: !@#$%^&*()",
                        "Unicode: 🚀🎉",
                        "Newlines:\nand\ttabs",
                    ],
                }
            )

            # Write DataFrame
            loader.write_deid_table(special_df, "special_table")

            # Read it back
            read_df = loader.read_table("special_table")
            pd.testing.assert_frame_equal(read_df, special_df)

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_filesystem_nonexistent_directory_creation(self):
        """Test FileSystemDataLoader creates non-existent directories."""
        temp_dir = tempfile.mkdtemp()
        subdir = Path(temp_dir) / "nonexistent" / "subdirectory"

        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {
                        "base_path": str(subdir),
                        "file_format": "csv",
                    },
                }
            )

            loader = FileSystemDataLoader(config)

            # Write to non-existent directory
            test_df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
            loader.write_deid_table(test_df, "test_table")

            # Verify directory was created and file exists
            assert subdir.exists()
            test_file = subdir / "test_table.csv"
            assert test_file.exists()

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_filesystem_permission_error(self):
        """Test FileSystemDataLoader handles permission errors gracefully."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {"base_path": temp_dir, "file_format": "csv"},
                }
            )

            loader = FileSystemDataLoader(config)

            # Create a file that can't be written to
            test_file = Path(temp_dir) / "test_table.csv"
            test_file.touch()
            test_file.chmod(0o444)  # Read-only

            test_df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

            with pytest.raises(WriteError, match="Failed to write file"):
                loader.write_deid_table(test_df, "test_table")

            # Restore permissions for cleanup
            test_file.chmod(0o644)

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_filesystem_corrupted_file(self):
        """Test FileSystemDataLoader handles corrupted files gracefully."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {"base_path": temp_dir, "file_format": "csv"},
                }
            )

            loader = FileSystemDataLoader(config)

            # Create corrupted CSV file
            test_file = Path(temp_dir) / "test_table.csv"
            with open(test_file, "w") as f:
                f.write(
                    "corrupted,csv,data\nwith,invalid\nstructure\nincomplete,line,extra,columns"
                )  # Inconsistent columns

            # Should raise FileFormatError when trying to read corrupted file
            with pytest.raises(
                FileFormatError, match=r"Failed to read file.*test_table"
            ):
                loader.read_table("test_table")

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.skipif(not SQL_AVAILABLE, reason="SQLAlchemy not available")
    def test_sql_connection_failure(self):
        """Test SQLDataLoader handles connection failures gracefully."""
        config = DictConfig(
            {
                "data_source_type": "sql",
                "connection_params": {
                    "database_url": "postgresql://invalid:invalid@invalid:5432/invalid"
                },
            }
        )

        with pytest.raises(IOConnectionError, match="Failed to connect to database"):
            SQLDataLoader(config)

    @pytest.mark.skipif(not SQL_AVAILABLE, reason="SQLAlchemy not available")
    def test_sql_missing_required_params(self):
        """Test SQLDataLoader with missing required parameters."""
        config = DictConfig(
            {
                "data_source_type": "sql",
                "connection_params": {
                    "driver": "postgresql",
                    "host": "localhost",
                    # Missing username, password, database
                },
            }
        )

        with pytest.raises(ValueError, match="Missing required connection parameters"):
            SQLDataLoader(config)

    @pytest.mark.skipif(not SQL_AVAILABLE, reason="SQLAlchemy not available")
    def test_sql_invalid_query(self):
        """Test SQLDataLoader with invalid SQL query."""
        config = DictConfig(
            {
                "data_source_type": "sql",
                "connection_params": {"database_url": "sqlite:///:memory:"},
            }
        )

        with SQLDataLoader(config) as loader:
            with pytest.raises(IOConnectionError, match="Failed to execute query"):
                loader.execute_query("INVALID SQL QUERY")

    def test_validation_error_scenarios(self):
        """Test various validation error scenarios."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": tempfile.mkdtemp(),
                    "file_format": "csv",
                },
                "validation_rules": {
                    "test_table": {
                        "required_columns": ["id", "name"],
                        "expected_types": {"id": "int64", "name": "object"},
                    }
                },
            }
        )

        loader = FileSystemDataLoader(config)

        # Test missing required columns
        df_missing_cols = pd.DataFrame({"id": [1, 2, 3]})
        with pytest.raises(ValueError, match="Missing required columns"):
            loader.validate_data(df_missing_cols, "test_table")

        # Test wrong data types
        df_wrong_types = pd.DataFrame(
            {
                "id": ["1", "2", "3"],  # Should be int64
                "name": ["A", "B", "C"],
            }
        )
        with pytest.raises(ValueError, match="has type 'object', expected 'int64'"):
            loader.validate_data(df_wrong_types, "test_table")

        # Test valid data
        df_valid = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        assert loader.validate_data(df_valid, "test_table") is True

    def test_table_mapping_edge_cases(self):
        """Test table mapping edge cases."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": tempfile.mkdtemp(),
                    "file_format": "csv",
                },
                "table_mappings": {
                    "patients": "patients_deid",
                    "encounters": "encounters_deid",
                },
                "suffix": "_deid",
            }
        )

        loader = FileSystemDataLoader(config)

        # Test existing mapping
        original, deid = loader.get_table_mapping("patients")
        assert original == "patients"
        assert deid == "patients_deid"

        # Test non-existing mapping (should return default)
        original, deid = loader.get_table_mapping("nonexistent")
        assert original == "nonexistent"
        assert deid == "nonexistent_deid"

        # Test empty string
        original, deid = loader.get_table_mapping("")
        assert original == ""
        assert deid == "_deid"

        # Test None (should work but not recommended)
        original, deid = loader.get_table_mapping(None)
        assert original is None
        assert deid == "None_deid"

    def test_config_edge_cases(self):
        """Test configuration edge cases."""
        # Test with None values
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": tempfile.mkdtemp(),
                    "file_format": "csv",
                    "encoding": None,
                    "separator": None,
                },
            }
        )

        loader = FileSystemDataLoader(config)

        # Should use defaults for None values
        assert loader.encoding == "utf-8"  # Default
        assert loader.separator == ","  # Default

    def test_context_manager_error_handling(self):
        """Test context manager error handling."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {"base_path": temp_dir, "file_format": "csv"},
                }
            )

            with FileSystemDataLoader(config) as loader:
                # This should work
                assert loader.data_source_type == "filesystem"

                # Simulate an error
                raise ValueError("Test error")

        except ValueError as e:
            # Error should be propagated
            assert str(e) == "Test error"

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_repr_edge_cases(self):
        """Test string representation edge cases."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": tempfile.mkdtemp(),
                    "file_format": "csv",
                },
            }
        )

        loader = FileSystemDataLoader(config)
        repr_str = repr(loader)

        assert "FileSystemDataLoader" in repr_str
        assert "filesystem" in repr_str

    @pytest.mark.skipif(
        not _has_parquet_support(),
        reason="Parquet support requires pyarrow or fastparquet",
    )
    def test_large_file_handling(self):
        """Test handling of large files."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {
                        "base_path": temp_dir,
                        "file_format": "parquet",
                    },
                }
            )

            loader = FileSystemDataLoader(config)

            # Create a large DataFrame
            large_df = pd.DataFrame(
                {
                    "id": range(50000),
                    "name": [f"Person_{i}" for i in range(50000)],
                    "value": [i * 0.1 for i in range(50000)],
                }
            )

            # Write large DataFrame
            loader.write_deid_table(large_df, "large_table")

            # Read it back
            read_df = loader.read_table("large_table")
            assert len(read_df) == 50000
            pd.testing.assert_frame_equal(read_df, large_df)

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_unicode_handling(self):
        """Test Unicode handling in file names and data."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {
                        "base_path": temp_dir,
                        "file_format": "csv",
                        "encoding": "utf-8",
                    },
                }
            )

            loader = FileSystemDataLoader(config)

            # Create DataFrame with Unicode data
            unicode_df = pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["José", "François", "北京"],
                    "description": [
                        "Special chars: !@#$%^&*()",
                        "Unicode: 🚀🎉",
                        "Newlines:\nand\ttabs",
                    ],
                }
            )

            # Write DataFrame
            loader.write_deid_table(unicode_df, "unicode_table")

            # Read it back
            read_df = loader.read_table("unicode_table")
            pd.testing.assert_frame_equal(read_df, unicode_df)

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_concurrent_access_simulation(self):
        """Test simulated concurrent access scenarios."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {"base_path": temp_dir, "file_format": "csv"},
                }
            )

            loader1 = FileSystemDataLoader(config)
            loader2 = FileSystemDataLoader(config)

            # Both loaders should work independently
            test_df1 = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
            test_df2 = pd.DataFrame({"id": [3, 4], "name": ["C", "D"]})

            loader1.write_deid_table(test_df1, "table1")
            loader2.write_deid_table(test_df2, "table2")

            # Both should be readable
            read_df1 = loader1.read_table("table1")
            read_df2 = loader2.read_table("table2")

            pd.testing.assert_frame_equal(read_df1, test_df1)
            pd.testing.assert_frame_equal(read_df2, test_df2)

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.skipif(
        not _has_parquet_support(),
        reason="Parquet support requires pyarrow or fastparquet",
    )
    def test_memory_usage_large_data(self):
        """Test memory usage with large data."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {
                        "base_path": temp_dir,
                        "file_format": "parquet",
                    },
                }
            )

            loader = FileSystemDataLoader(config)

            # Create large DataFrame
            large_df = pd.DataFrame(
                {
                    "id": range(100000),
                    "name": [f"Person_{i}" for i in range(100000)],
                    "value": [i * 0.1 for i in range(100000)],
                }
            )

            # Write large DataFrame
            loader.write_deid_table(large_df, "large_table")

            # Read it back
            read_df = loader.read_table("large_table")
            assert len(read_df) == 100000

            # Verify data integrity
            assert read_df.iloc[0]["id"] == 0
            assert read_df.iloc[-1]["id"] == 99999

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_error_propagation(self):
        """Test that errors are properly propagated."""
        temp_dir = tempfile.mkdtemp()
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {"base_path": temp_dir, "file_format": "csv"},
                }
            )

            loader = FileSystemDataLoader(config)

            # Test that TableNotFoundError is raised for read errors
            with pytest.raises(TableNotFoundError):
                loader.read_table("nonexistent_table")

            # Test that WriteError is raised for write errors
            with pytest.raises(WriteError):
                # Create a file that can't be written to
                test_file = Path(temp_dir) / "test_table.csv"
                test_file.touch()
                test_file.chmod(0o444)  # Read-only

                try:
                    loader.write_deid_table(pd.DataFrame(), "test_table")
                finally:
                    test_file.chmod(0o644)  # Restore permissions

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)
