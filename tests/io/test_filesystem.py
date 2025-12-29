"""Unit tests for FileSystemDataLoader."""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from omegaconf import DictConfig

from cleared.io.filesystem import FileSystemDataLoader
from cleared.io.base import (
    IOConnectionError,
    TableNotFoundError,
    WriteError,
    FileFormatError,
)


def _has_parquet_support():
    """Check if parquet support is available."""
    import importlib.util

    return (
        importlib.util.find_spec("pyarrow") is not None
        or importlib.util.find_spec("fastparquet") is not None
    )


def _has_excel_support():
    """Check if Excel support is available."""
    import importlib.util

    return (
        importlib.util.find_spec("openpyxl") is not None
        or importlib.util.find_spec("xlrd") is not None
    )


class TestFileSystemDataLoader:
    """Test the FileSystemDataLoader class."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "csv",
                    "encoding": "utf-8",
                    "separator": ",",
                },
            }
        )

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization_with_valid_config(self):
        """Test initialization with valid configuration."""
        loader = FileSystemDataLoader(self.config)

        assert loader.data_source_type == "filesystem"
        assert loader.base_path == Path(self.temp_dir)
        assert loader.file_format == "csv"
        assert loader.encoding == "utf-8"
        assert loader.separator == ","

    def test_initialization_with_defaults(self):
        """Test initialization with minimal configuration."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {"base_path": self.temp_dir},
            }
        )

        loader = FileSystemDataLoader(config)

        assert loader.file_format == "csv"
        assert loader.encoding == "utf-8"
        assert loader.separator == ","

    def test_initialization_nonexistent_path(self):
        """Test initialization fails with non-existent path."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {"base_path": "/nonexistent/path"},
            }
        )

        with pytest.raises(IOConnectionError, match="Failed to create base path"):
            FileSystemDataLoader(config)

    def test_initialization_file_not_directory(self):
        """Test initialization fails when path is a file, not directory."""
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()

        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {"base_path": temp_file.name},
                }
            )

            with pytest.raises(IOConnectionError, match="Base path is not a directory"):
                FileSystemDataLoader(config)
        finally:
            os.unlink(temp_file.name)

    def test_get_file_path(self):
        """Test file path generation."""
        loader = FileSystemDataLoader(self.config)

        expected_path = Path(self.temp_dir) / "test_table.csv"
        assert loader._get_file_path("test_table") == expected_path

    def test_get_file_path_different_format(self):
        """Test file path generation with different format."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "parquet",
                },
            }
        )

        loader = FileSystemDataLoader(config)

        expected_path = Path(self.temp_dir) / "test_table.parquet"
        assert loader._get_file_path("test_table") == expected_path

    def test_read_table_csv_success(self):
        """Test reading CSV table successfully."""
        # Create test CSV file
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        test_file = Path(self.temp_dir) / "test_table.csv"
        test_data.to_csv(test_file, index=False)

        loader = FileSystemDataLoader(self.config)
        result = loader.read_table("test_table")

        pd.testing.assert_frame_equal(result, test_data)

    def test_read_table_csv_with_custom_separator(self):
        """Test reading CSV with custom separator."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "csv",
                    "separator": "|",
                },
            }
        )

        # Create test CSV file with pipe separator
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        test_file = Path(self.temp_dir) / "test_table.csv"
        test_data.to_csv(test_file, index=False, sep="|")

        loader = FileSystemDataLoader(config)
        result = loader.read_table("test_table")

        pd.testing.assert_frame_equal(result, test_data)

    def test_read_table_parquet_success(self):
        """Test reading Parquet table successfully."""
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {
                        "base_path": self.temp_dir,
                        "file_format": "parquet",
                    },
                }
            )

            # Create test Parquet file
            test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
            test_file = Path(self.temp_dir) / "test_table.parquet"
            test_data.to_parquet(test_file, index=False)

            loader = FileSystemDataLoader(config)
            result = loader.read_table("test_table")

            pd.testing.assert_frame_equal(result, test_data)
        except ImportError:
            pytest.skip("Parquet dependencies not available")

    def test_read_table_json_success(self):
        """Test reading JSON table successfully."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "json",
                },
            }
        )

        # Create test JSON file
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        test_file = Path(self.temp_dir) / "test_table.json"
        test_data.to_json(test_file, orient="records", index=False)

        loader = FileSystemDataLoader(config)
        result = loader.read_table("test_table")

        pd.testing.assert_frame_equal(result, test_data)

    def test_read_table_excel_success(self):
        """Test reading Excel table successfully."""
        try:
            config = DictConfig(
                {
                    "data_source_type": "filesystem",
                    "connection_params": {
                        "base_path": self.temp_dir,
                        "file_format": "xlsx",
                    },
                }
            )

            # Create test Excel file
            test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
            test_file = Path(self.temp_dir) / "test_table.xlsx"
            test_data.to_excel(test_file, index=False)

            loader = FileSystemDataLoader(config)
            result = loader.read_table("test_table")

            pd.testing.assert_frame_equal(result, test_data)
        except ImportError:
            pytest.skip("Excel dependencies not available")

    def test_read_table_pickle_success(self):
        """Test reading Pickle table successfully."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "pickle",
                },
            }
        )

        # Create test Pickle file
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        test_file = Path(self.temp_dir) / "test_table.pkl"  # Use .pkl extension
        test_data.to_pickle(test_file)

        loader = FileSystemDataLoader(config)
        result = loader.read_table("test_table")

        pd.testing.assert_frame_equal(result, test_data)

    def test_read_table_file_not_found(self):
        """Test reading non-existent table raises TableNotFoundError."""
        loader = FileSystemDataLoader(self.config)

        with pytest.raises(TableNotFoundError, match="Table file not found"):
            loader.read_table("nonexistent_table")

    def test_read_table_unsupported_format(self):
        """Test reading with unsupported format raises ConnectionError."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "unsupported",
                },
            }
        )

        loader = FileSystemDataLoader(config)

        # Create a file with unsupported extension
        test_file = Path(self.temp_dir) / "test_table.unsupported"
        test_file.touch()

        with pytest.raises(FileFormatError, match="Unsupported file format"):
            loader.read_table("test_table")

    def test_read_table_read_error(self):
        """Test reading corrupted file raises FileFormatError."""
        loader = FileSystemDataLoader(self.config)

        # Create a file that will cause a pandas error
        test_file = Path(self.temp_dir) / "test_table.csv"
        with open(test_file, "w") as f:
            f.write("a,b,c\n1,2\n3,4,5,6")  # Inconsistent column count

        with pytest.raises(FileFormatError, match="Failed to read file"):
            loader.read_table("test_table")

    def test_write_deid_table_csv_replace(self):
        """Test writing CSV table with replace mode."""
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        loader = FileSystemDataLoader(self.config)
        loader.write_deid_table(test_data, "test_table", if_exists="replace")

        # Verify file was created
        test_file = Path(self.temp_dir) / "test_table.csv"
        assert test_file.exists()

        # Verify content
        result = pd.read_csv(test_file)
        pd.testing.assert_frame_equal(result, test_data)

    def test_write_deid_table_csv_append(self):
        """Test writing CSV table with append mode."""
        # Create initial file
        initial_data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        test_file = Path(self.temp_dir) / "test_table.csv"
        initial_data.to_csv(test_file, index=False)

        # Append new data
        new_data = pd.DataFrame({"id": [3, 4], "name": ["C", "D"]})

        loader = FileSystemDataLoader(self.config)
        loader.write_deid_table(new_data, "test_table", if_exists="append")

        # Verify content
        result = pd.read_csv(test_file)
        expected = pd.concat([initial_data, new_data], ignore_index=True)
        pd.testing.assert_frame_equal(result, expected)

    def test_write_deid_table_csv_fail(self):
        """Test writing CSV table with fail mode when file exists."""
        # Create initial file
        initial_data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        test_file = Path(self.temp_dir) / "test_table.csv"
        initial_data.to_csv(test_file, index=False)

        new_data = pd.DataFrame({"id": [3, 4], "name": ["C", "D"]})

        loader = FileSystemDataLoader(self.config)

        with pytest.raises(WriteError, match="File already exists"):
            loader.write_deid_table(new_data, "test_table", if_exists="fail")

    @pytest.mark.skipif(
        not _has_parquet_support(),
        reason="Parquet support requires pyarrow or fastparquet",
    )
    def test_write_deid_table_parquet(self):
        """Test writing Parquet table."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "parquet",
                },
            }
        )

        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        loader = FileSystemDataLoader(config)
        loader.write_deid_table(test_data, "test_table")

        # Verify file was created
        test_file = Path(self.temp_dir) / "test_table.parquet"
        assert test_file.exists()

        # Verify content
        result = pd.read_parquet(test_file)
        pd.testing.assert_frame_equal(result, test_data)

    def test_write_deid_table_json(self):
        """Test writing JSON table."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "json",
                },
            }
        )

        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        loader = FileSystemDataLoader(config)
        loader.write_deid_table(test_data, "test_table")

        # Verify file was created
        test_file = Path(self.temp_dir) / "test_table.json"
        assert test_file.exists()

        # Verify content
        result = pd.read_json(test_file)
        pd.testing.assert_frame_equal(result, test_data)

    @pytest.mark.skipif(
        not _has_excel_support(), reason="Excel support requires openpyxl or xlrd"
    )
    def test_write_deid_table_excel(self):
        """Test writing Excel table."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "xlsx",
                },
            }
        )

        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        loader = FileSystemDataLoader(config)
        loader.write_deid_table(test_data, "test_table")

        # Verify file was created
        test_file = Path(self.temp_dir) / "test_table.xlsx"
        assert test_file.exists()

        # Verify content
        result = pd.read_excel(test_file)
        pd.testing.assert_frame_equal(result, test_data)

    def test_write_deid_table_pickle(self):
        """Test writing Pickle table."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "pickle",
                },
            }
        )

        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        loader = FileSystemDataLoader(config)
        loader.write_deid_table(test_data, "test_table")

        # Verify file was created
        test_file = Path(self.temp_dir) / "test_table.pkl"
        assert test_file.exists()

        # Verify content
        result = pd.read_pickle(test_file)
        pd.testing.assert_frame_equal(result, test_data)

    def test_write_deid_table_unsupported_format(self):
        """Test writing with unsupported format raises WriteError."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "unsupported",
                },
            }
        )

        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        loader = FileSystemDataLoader(config)

        with pytest.raises(WriteError, match="Unsupported file format"):
            loader.write_deid_table(test_data, "test_table")

    def test_write_deid_table_with_index(self):
        """Test writing table with index."""
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        test_data.index = [10, 20, 30]  # Set custom index

        loader = FileSystemDataLoader(self.config)
        loader.write_deid_table(test_data, "test_table", index=True)

        # Verify content includes index
        test_file = Path(self.temp_dir) / "test_table.csv"
        result = pd.read_csv(test_file, index_col=0)
        pd.testing.assert_frame_equal(result, test_data)

    def test_write_deid_table_create_directory(self):
        """Test writing to non-existent directory creates it."""
        subdir = Path(self.temp_dir) / "subdir"
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {"base_path": str(subdir), "file_format": "csv"},
            }
        )

        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        loader = FileSystemDataLoader(config)
        loader.write_deid_table(test_data, "test_table")

        # Verify directory was created and file exists
        assert subdir.exists()
        test_file = subdir / "test_table.csv"
        assert test_file.exists()

    def test_write_deid_table_write_error(self):
        """Test writing fails with WriteError on file system error."""
        # Create a file that can't be written to (simulate permission error)
        test_file = Path(self.temp_dir) / "test_table.csv"
        test_file.touch()
        test_file.chmod(0o444)  # Read-only

        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        loader = FileSystemDataLoader(self.config)

        with pytest.raises(WriteError, match="Failed to write file"):
            loader.write_deid_table(test_data, "test_table")

        # Restore permissions for cleanup
        test_file.chmod(0o644)

    def test_list_tables(self):
        """Test listing available tables."""
        # Create test files
        test_files = ["table1.csv", "table2.csv", "table3.csv"]
        for filename in test_files:
            file_path = Path(self.temp_dir) / filename
            pd.DataFrame({"id": [1, 2, 3]}).to_csv(file_path, index=False)

        loader = FileSystemDataLoader(self.config)
        tables = loader.list_tables()

        assert set(tables) == {"table1", "table2", "table3"}
        assert tables == sorted(tables)  # Should be sorted

    @pytest.mark.skipif(
        not _has_parquet_support(),
        reason="Parquet support requires pyarrow or fastparquet",
    )
    def test_list_tables_different_format(self):
        """Test listing tables with different file format."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "parquet",
                },
            }
        )

        # Create test files with different extensions
        test_files = ["table1.parquet", "table2.parquet", "table3.csv"]
        for filename in test_files:
            file_path = Path(self.temp_dir) / filename
            if filename.endswith(".parquet"):
                pd.DataFrame({"id": [1, 2, 3]}).to_parquet(file_path, index=False)
            else:
                pd.DataFrame({"id": [1, 2, 3]}).to_csv(file_path, index=False)

        loader = FileSystemDataLoader(config)
        tables = loader.list_tables()

        # Should only include .parquet files
        assert set(tables) == {"table1", "table2"}

    def test_list_tables_empty_directory(self):
        """Test listing tables in empty directory."""
        loader = FileSystemDataLoader(self.config)
        tables = loader.list_tables()

        assert tables == []

    def test_close_connection(self):
        """Test close_connection method."""
        loader = FileSystemDataLoader(self.config)

        # Should not raise any exception
        loader.close_connection()

    def test_context_manager(self):
        """Test context manager functionality."""
        with FileSystemDataLoader(self.config) as loader:
            assert loader.data_source_type == "filesystem"
            assert loader.base_path == Path(self.temp_dir)

    def test_repr(self):
        """Test string representation."""
        loader = FileSystemDataLoader(self.config)

        expected = "FileSystemDataLoader(data_source_type='filesystem')"
        assert repr(loader) == expected

    def test_get_table_paths_single_file(self):
        """Test get_table_paths() returns single Path for existing file."""
        loader = FileSystemDataLoader(self.config)

        # Create a test CSV file
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        test_file = Path(self.temp_dir) / "test_table.csv"
        test_data.to_csv(test_file, index=False)

        # Test get_table_paths returns single Path
        result = loader.get_table_paths("test_table")
        assert isinstance(result, Path)
        assert result == test_file

    def test_get_table_paths_directory(self):
        """Test get_table_paths() returns list of Paths for directory."""
        loader = FileSystemDataLoader(self.config)

        # Create directory with segment files
        table_dir = Path(self.temp_dir) / "users"
        table_dir.mkdir()

        # Create multiple segment files
        segment1 = table_dir / "segment1.csv"
        segment2 = table_dir / "segment2.csv"
        segment3 = table_dir / "segment3.csv"

        pd.DataFrame({"id": [1, 2], "name": ["A", "B"]}).to_csv(segment1, index=False)
        pd.DataFrame({"id": [3, 4], "name": ["C", "D"]}).to_csv(segment2, index=False)
        pd.DataFrame({"id": [5, 6], "name": ["E", "F"]}).to_csv(segment3, index=False)

        # Test get_table_paths returns list of Paths
        result = loader.get_table_paths("users")
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(p, Path) for p in result)
        assert segment1 in result
        assert segment2 in result
        assert segment3 in result

    def test_get_table_paths_not_found(self):
        """Test TableNotFoundError when neither file nor directory exists."""
        loader = FileSystemDataLoader(self.config)

        with pytest.raises(TableNotFoundError):
            loader.get_table_paths("nonexistent_table")

    def test_get_table_paths_empty_directory(self):
        """Test empty directory raises TableNotFoundError."""
        loader = FileSystemDataLoader(self.config)

        # Create empty directory
        table_dir = Path(self.temp_dir) / "empty_table"
        table_dir.mkdir()

        with pytest.raises(TableNotFoundError, match="contains no files"):
            loader.get_table_paths("empty_table")

    def test_get_table_paths_file_takes_precedence(self):
        """Test file takes precedence over directory if both exist."""
        loader = FileSystemDataLoader(self.config)

        # Create both file and directory with same name
        test_file = Path(self.temp_dir) / "test_table.csv"
        pd.DataFrame({"id": [1], "name": ["A"]}).to_csv(test_file, index=False)

        table_dir = Path(self.temp_dir) / "test_table"
        table_dir.mkdir()
        segment = table_dir / "segment1.csv"
        pd.DataFrame({"id": [2], "name": ["B"]}).to_csv(segment, index=False)

        # File should take precedence
        result = loader.get_table_paths("test_table")
        assert isinstance(result, Path)
        assert result == test_file

    def test_read_table_with_segment_path(self):
        """Test read_table() with segment_path parameter."""
        loader = FileSystemDataLoader(self.config)

        # Create a segment file directly
        segment_file = Path(self.temp_dir) / "custom_segment.csv"
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        test_data.to_csv(segment_file, index=False)

        # Read using segment_path
        result = loader.read_table("test_table", segment_path=segment_file)
        pd.testing.assert_frame_equal(result, test_data)

    def test_read_table_with_segment_path_different_format(self):
        """Test read_table() with segment_path detects format from extension."""
        loader = FileSystemDataLoader(self.config)

        # Create JSON segment file
        segment_file = Path(self.temp_dir) / "custom_segment.json"
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        test_data.to_json(segment_file, orient="records", index=False)

        # Read using segment_path - should detect JSON format
        result = loader.read_table("test_table", segment_path=segment_file)
        pd.testing.assert_frame_equal(result, test_data)

    def test_write_deid_table_with_segment_name(self):
        """Test write_deid_table() with segment_name parameter."""
        loader = FileSystemDataLoader(self.config)

        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        # Write with segment_name
        loader.write_deid_table(test_data, "users", segment_name="segment1.csv")

        # Verify directory structure created
        output_dir = Path(self.temp_dir) / "users"
        assert output_dir.exists()
        assert output_dir.is_dir()

        # Verify segment file created
        segment_file = output_dir / "segment1.csv"
        assert segment_file.exists()

        # Verify content
        read_data = pd.read_csv(segment_file)
        pd.testing.assert_frame_equal(read_data, test_data)

    def test_write_deid_table_with_segment_name_multiple_segments(self):
        """Test writing multiple segments to same table directory."""
        loader = FileSystemDataLoader(self.config)

        segment1_data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        segment2_data = pd.DataFrame({"id": [3, 4], "name": ["C", "D"]})

        # Write multiple segments
        loader.write_deid_table(segment1_data, "users", segment_name="segment1.csv")
        loader.write_deid_table(segment2_data, "users", segment_name="segment2.csv")

        # Verify both segments exist
        output_dir = Path(self.temp_dir) / "users"
        assert (output_dir / "segment1.csv").exists()
        assert (output_dir / "segment2.csv").exists()

        # Verify contents
        read1 = pd.read_csv(output_dir / "segment1.csv")
        read2 = pd.read_csv(output_dir / "segment2.csv")
        pd.testing.assert_frame_equal(read1, segment1_data)
        pd.testing.assert_frame_equal(read2, segment2_data)

    def test_detect_file_format_from_path(self):
        """Test format detection for various extensions."""
        loader = FileSystemDataLoader(self.config)

        test_cases = [
            ("file.csv", "csv"),
            ("file.parquet", "parquet"),
            ("file.json", "json"),
            ("file.xlsx", "xlsx"),
            ("file.xls", "xls"),
            ("file.pkl", "pickle"),
            ("file.unknown", "csv"),  # Default fallback
        ]

        for file_path_str, expected_format in test_cases:
            file_path = Path(file_path_str)
            detected_format = loader._detect_file_format_from_path(file_path)
            assert detected_format == expected_format, f"Failed for {file_path_str}"

    def test_mixed_file_types_in_directory(self):
        """Test directory with CSV, Parquet, JSON files."""
        loader = FileSystemDataLoader(self.config)

        # Create directory with mixed file types
        table_dir = Path(self.temp_dir) / "mixed_table"
        table_dir.mkdir()

        # Create CSV file
        csv_file = table_dir / "segment1.csv"
        csv_data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        csv_data.to_csv(csv_file, index=False)

        # Create JSON file
        json_file = table_dir / "segment2.json"
        json_data = pd.DataFrame({"id": [3, 4], "name": ["C", "D"]})
        json_data.to_json(json_file, orient="records", index=False)

        # Get paths - should return all files
        paths = loader.get_table_paths("mixed_table")
        assert isinstance(paths, list)
        assert len(paths) == 2
        assert csv_file in paths
        assert json_file in paths

        # Test reading each segment
        csv_result = loader.read_table("mixed_table", segment_path=csv_file)
        json_result = loader.read_table("mixed_table", segment_path=json_file)

        pd.testing.assert_frame_equal(csv_result, csv_data)
        pd.testing.assert_frame_equal(json_result, json_data)


class TestFileSystemDataLoaderIntegration:
    """Integration tests for FileSystemDataLoader."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_full_workflow_csv(self):
        """Test complete CSV workflow."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "csv",
                    "encoding": "utf-8",
                    "separator": ",",
                },
                "table_mappings": {"patients": "patients_deid"},
                "validation_rules": {
                    "patients": {
                        "required_columns": ["id", "name"],
                        "expected_types": {"id": "int64"},
                    }
                },
            }
        )

        with FileSystemDataLoader(config) as loader:
            # Create initial data
            original_data = pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )

            # Write original data
            loader.write_deid_table(original_data, "patients")

            # Read data back
            read_data = loader.read_table("patients")
            pd.testing.assert_frame_equal(read_data, original_data)

            # Validate data
            assert loader.validate_data(read_data, "patients") is True

            # Test table mapping
            original, deid = loader.get_table_mapping("patients")
            assert original == "patients"
            assert deid == "patients_deid"

            # List tables
            tables = loader.list_tables()
            assert "patients" in tables

    def test_multiple_formats(self):
        """Test working with multiple file formats."""
        base_config = {
            "data_source_type": "filesystem",
            "connection_params": {"base_path": self.temp_dir},
        }

        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        # Test different formats (skip optional dependencies)
        formats = ["csv", "json", "pickle"]
        if _has_parquet_support():
            formats.append("parquet")
        if _has_excel_support():
            formats.append("xlsx")

        for fmt in formats:
            config = DictConfig(
                {
                    **base_config,
                    "connection_params": {
                        **base_config["connection_params"],
                        "file_format": fmt,
                    },
                }
            )

            loader = FileSystemDataLoader(config)

            # Write data
            loader.write_deid_table(test_data, f"test_{fmt}")

            # Read data back
            read_data = loader.read_table(f"test_{fmt}")
            pd.testing.assert_frame_equal(read_data, test_data)

    @pytest.mark.skipif(
        not _has_parquet_support(),
        reason="Parquet support requires pyarrow or fastparquet",
    )
    def test_large_dataset(self):
        """Test handling large dataset."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "parquet",  # Use parquet for better performance
                },
            }
        )

        # Create large dataset
        large_data = pd.DataFrame(
            {
                "id": range(10000),
                "name": [f"Person_{i}" for i in range(10000)],
                "value": [i * 0.1 for i in range(10000)],
            }
        )

        loader = FileSystemDataLoader(config)

        # Write large dataset
        loader.write_deid_table(large_data, "large_table")

        # Read large dataset
        read_data = loader.read_table("large_table")

        assert len(read_data) == 10000
        pd.testing.assert_frame_equal(read_data, large_data)

    def test_append_workflow(self):
        """Test append workflow with multiple writes."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {"base_path": self.temp_dir, "file_format": "csv"},
            }
        )

        loader = FileSystemDataLoader(config)

        # Initial data
        initial_data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        loader.write_deid_table(initial_data, "test_table", if_exists="replace")

        # Append more data
        additional_data = pd.DataFrame({"id": [3, 4], "name": ["C", "D"]})
        loader.write_deid_table(additional_data, "test_table", if_exists="append")

        # Read final data
        final_data = loader.read_table("test_table")
        expected_data = pd.concat([initial_data, additional_data], ignore_index=True)

        pd.testing.assert_frame_equal(final_data, expected_data)

    def test_segment_workflow(self):
        """Test full workflow with segment directory structure."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": self.temp_dir,
                    "file_format": "csv",
                },
            }
        )

        _loader = FileSystemDataLoader(config)

        # Create input directory with segments
        input_dir = Path(self.temp_dir) / "input"
        input_dir.mkdir()
        users_dir = input_dir / "users"
        users_dir.mkdir()

        # Create multiple segment files
        segment1_data = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        segment2_data = pd.DataFrame({"id": [3, 4], "name": ["Charlie", "Diana"]})
        segment3_data = pd.DataFrame({"id": [5, 6], "name": ["Eve", "Frank"]})

        segment1_data.to_csv(users_dir / "segment1.csv", index=False)
        segment2_data.to_csv(users_dir / "segment2.csv", index=False)
        segment3_data.to_csv(users_dir / "segment3.csv", index=False)

        # Create loader for input directory
        input_config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": str(input_dir),
                    "file_format": "csv",
                },
            }
        )
        input_loader = FileSystemDataLoader(input_config)

        # Create output directory
        output_dir = Path(self.temp_dir) / "output"
        output_dir.mkdir()
        output_config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": str(output_dir),
                    "file_format": "csv",
                },
            }
        )
        output_loader = FileSystemDataLoader(output_config)

        # Read all segments and write them
        segment_paths = input_loader.get_table_paths("users")
        assert isinstance(segment_paths, list)
        assert len(segment_paths) == 3

        for segment_path in segment_paths:
            segment_data = input_loader.read_table("users", segment_path=segment_path)
            segment_name = segment_path.name
            output_loader.write_deid_table(
                segment_data, "users", segment_name=segment_name
            )

        # Verify output structure matches input
        output_users_dir = output_dir / "users"
        assert output_users_dir.exists()
        assert output_users_dir.is_dir()

        assert (output_users_dir / "segment1.csv").exists()
        assert (output_users_dir / "segment2.csv").exists()
        assert (output_users_dir / "segment3.csv").exists()

        # Verify contents match
        for segment_path in segment_paths:
            segment_name = segment_path.name
            original_data = pd.read_csv(segment_path)
            output_data = pd.read_csv(output_users_dir / segment_name)
            pd.testing.assert_frame_equal(original_data, output_data)

    def test_table_mapping_functionality(self):
        """Test table mapping functionality in FileSystemDataLoader."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {"base_path": self.temp_dir, "file_format": "csv"},
                "table_mappings": {
                    "patients": "patients_deid",
                    "encounters": "encounters_deid",
                },
                "suffix": "_deid",
            }
        )

        loader = FileSystemDataLoader(config)

        # Test table mapping methods
        assert loader.get_deid_table_name("patients") == "patients_deid"
        assert loader.get_deid_table_name("encounters") == "encounters_deid"
        assert loader.get_deid_table_name("other_table") == "other_table_deid"

        assert loader.get_original_table_name("patients_deid") == "patients"
        assert loader.get_original_table_name("encounters_deid") == "encounters"
        assert loader.get_original_table_name("other_table_deid") == "other_table"

        original, deid = loader.get_table_mapping("patients")
        assert original == "patients"
        assert deid == "patients_deid"

    def test_list_original_tables(self):
        """Test listing original table names."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {"base_path": self.temp_dir, "file_format": "csv"},
                "table_mappings": {"patients": "patients_deid"},
                "suffix": "_deid",
            }
        )

        # Create test files
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        # Create original table
        test_data.to_csv(Path(self.temp_dir) / "patients.csv", index=False)

        # Create deid table
        test_data.to_csv(Path(self.temp_dir) / "patients_deid.csv", index=False)

        # Create other table
        test_data.to_csv(Path(self.temp_dir) / "other_table.csv", index=False)

        loader = FileSystemDataLoader(config)

        # Test list_original_tables
        original_tables = loader.list_original_tables()
        assert "patients" in original_tables
        assert "other_table" in original_tables
        assert (
            "patients_deid" not in original_tables
        )  # Should be mapped back to "patients"

    def test_list_deid_tables(self):
        """Test listing de-identified table names."""
        config = DictConfig(
            {
                "data_source_type": "filesystem",
                "connection_params": {"base_path": self.temp_dir, "file_format": "csv"},
                "table_mappings": {"patients": "patients_deid"},
                "suffix": "_deid",
            }
        )

        # Create test files
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        # Create original table
        test_data.to_csv(Path(self.temp_dir) / "patients.csv", index=False)

        # Create deid table
        test_data.to_csv(Path(self.temp_dir) / "patients_deid.csv", index=False)

        # Create other table with suffix
        test_data.to_csv(Path(self.temp_dir) / "other_table_deid.csv", index=False)

        # Create regular table
        test_data.to_csv(Path(self.temp_dir) / "regular_table.csv", index=False)

        loader = FileSystemDataLoader(config)

        # Test list_deid_tables
        deid_tables = loader.list_deid_tables()
        assert "patients_deid" in deid_tables  # Mapped table
        assert "other_table_deid" in deid_tables  # Suffixed table
        assert "patients" not in deid_tables  # Original table
        assert "regular_table" not in deid_tables  # Regular table without suffix
