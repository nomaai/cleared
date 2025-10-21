"""Unit tests for data loader configuration issues encountered during CLI development."""

import pytest
import tempfile
from unittest.mock import patch
from pathlib import Path

from cleared.io import create_data_loader
from cleared.io.filesystem import FileSystemDataLoader
from cleared.io.sql import SQLDataLoader
from cleared.config.structure import IOConfig


class TestDataLoaderConfiguration:
    """Test data loader configuration handling."""

    def test_create_filesystem_data_loader_with_io_config(self):
        """
        Test creating filesystem data loader with IOConfig object.

        Issue: Data loaders expected 'data_source_type' in config but received IOConfig object.
        """
        io_config = IOConfig(
            io_type="filesystem",
            configs={
                "base_path": "/tmp/test",
                "file_format": "csv",
                "encoding": "utf-8",
                "separator": ",",
            },
        )

        data_loader = create_data_loader(io_config)

        # Verify it's a filesystem data loader
        assert isinstance(data_loader, FileSystemDataLoader)

        # Verify configuration was properly converted
        assert data_loader.data_source_type == "filesystem"
        assert data_loader.connection_params["base_path"] == "/tmp/test"
        assert data_loader.connection_params["file_format"] == "csv"
        assert data_loader.connection_params["encoding"] == "utf-8"
        assert data_loader.connection_params["separator"] == ","

        # Verify base path and file format are set correctly
        assert data_loader.base_path == Path("/tmp/test")
        assert data_loader.file_format == "csv"
        assert data_loader.encoding == "utf-8"
        assert data_loader.separator == ","

    def test_create_sql_data_loader_with_io_config(self):
        """Test creating SQL data loader with IOConfig object."""
        io_config = IOConfig(
            io_type="sql",
            configs={
                "connection_string": "sqlite:///test.db",
                "table_mappings": {"patients": "patients_deid"},
                "validation_rules": {"patients": {"required_columns": ["id"]}},
            },
        )

        with patch("cleared.io.sql.SQLDataLoader._initialize_connection"):
            data_loader = create_data_loader(io_config)

        # Verify it's a SQL data loader
        assert isinstance(data_loader, SQLDataLoader)

        # Verify configuration was properly converted
        assert data_loader.data_source_type == "sql"
        assert data_loader.connection_params["connection_string"] == "sqlite:///test.db"
        assert data_loader.connection_params["table_mappings"] == {
            "patients": "patients_deid"
        }
        assert data_loader.connection_params["validation_rules"] == {
            "patients": {"required_columns": ["id"]}
        }

    def test_create_data_loader_with_unsupported_type(self):
        """Test creating data loader with unsupported IO type."""
        io_config = IOConfig(io_type="unsupported", configs={})

        with pytest.raises(ValueError, match="Unsupported IO type: unsupported"):
            create_data_loader(io_config)

    def test_data_loader_config_structure_conversion(self):
        """Test that IOConfig is properly converted to expected data loader config structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            io_config = IOConfig(
                io_type="filesystem",
                configs={
                    "base_path": temp_dir,
                    "file_format": "parquet",
                    "encoding": "latin-1",
                    "separator": ";",
                },
            )

            data_loader = create_data_loader(io_config)

            # Verify the internal config structure
            expected_config = {
                "data_source_type": "filesystem",
                "connection_params": {
                    "base_path": temp_dir,
                    "file_format": "parquet",
                    "encoding": "latin-1",
                    "separator": ";",
                },
            }

            assert data_loader.config == expected_config

    def test_filesystem_data_loader_initialization_with_dict_config(self):
        """Test that FileSystemDataLoader can be initialized with dictionary config."""
        config = {
            "data_source_type": "filesystem",
            "connection_params": {
                "base_path": "/tmp/test",
                "file_format": "csv",
                "encoding": "utf-8",
                "separator": ",",
            },
        }

        data_loader = FileSystemDataLoader(config)

        # Verify initialization
        assert data_loader.data_source_type == "filesystem"
        assert data_loader.base_path == Path("/tmp/test")
        assert data_loader.file_format == "csv"
        assert data_loader.encoding == "utf-8"
        assert data_loader.separator == ","

    def test_sql_data_loader_initialization_with_dict_config(self):
        """Test that SQLDataLoader can be initialized with dictionary config."""
        config = {
            "data_source_type": "sql",
            "connection_params": {
                "connection_string": "sqlite:///test.db",
                "table_mappings": {"patients": "patients_deid"},
                "validation_rules": {"patients": {"required_columns": ["id"]}},
            },
        }

        with patch("cleared.io.sql.SQLDataLoader._initialize_connection"):
            data_loader = SQLDataLoader(config)

        # Verify initialization
        assert data_loader.data_source_type == "sql"
        assert data_loader.connection_params["connection_string"] == "sqlite:///test.db"
        assert data_loader.connection_params["table_mappings"] == {
            "patients": "patients_deid"
        }

    def test_data_loader_config_with_missing_connection_params(self):
        """Test data loader with missing connection parameters."""
        config = {
            "data_source_type": "filesystem",
            "connection_params": {},  # Empty connection params
        }

        data_loader = FileSystemDataLoader(config)

        # Should use defaults
        assert data_loader.data_source_type == "filesystem"
        assert data_loader.base_path == Path(".")  # Default base path
        assert data_loader.file_format == "csv"  # Default file format

    def test_data_loader_config_with_none_connection_params(self):
        """Test data loader with None connection parameters."""
        config = {"data_source_type": "filesystem", "connection_params": None}

        with pytest.raises(
            AttributeError, match="'NoneType' object has no attribute 'get'"
        ):
            FileSystemDataLoader(config)

    def test_create_data_loader_preserves_original_io_config(self):
        """Test that create_data_loader doesn't modify the original IOConfig."""
        original_configs = {
            "base_path": "/tmp/test",
            "file_format": "csv",
            "encoding": "utf-8",
            "separator": ",",
        }

        io_config = IOConfig(io_type="filesystem", configs=original_configs.copy())

        # Create data loader
        data_loader = create_data_loader(io_config)

        # Verify original IOConfig is unchanged
        assert io_config.configs == original_configs
        assert io_config.io_type == "filesystem"

        # Verify data loader has correct config
        assert data_loader.connection_params["base_path"] == "/tmp/test"

    def test_create_data_loader_with_empty_configs(self):
        """Test creating data loader with empty configs dictionary."""
        io_config = IOConfig(io_type="filesystem", configs={})

        data_loader = create_data_loader(io_config)

        # Should still create data loader with defaults
        assert isinstance(data_loader, FileSystemDataLoader)
        assert data_loader.data_source_type == "filesystem"
        assert data_loader.connection_params == {}

    def test_create_data_loader_with_none_configs(self):
        """Test creating data loader with None configs."""
        io_config = IOConfig(io_type="filesystem", configs=None)

        with pytest.raises(
            AttributeError, match="'NoneType' object has no attribute 'get'"
        ):
            create_data_loader(io_config)


class TestDataLoaderErrorHandling:
    """Test error handling in data loader creation."""

    def test_create_data_loader_with_invalid_io_config_type(self):
        """Test creating data loader with invalid IOConfig type."""
        # Pass a string instead of IOConfig
        with pytest.raises(AttributeError):
            create_data_loader("invalid_config")

    def test_create_data_loader_with_none_io_config(self):
        """Test creating data loader with None IOConfig."""
        with pytest.raises(AttributeError):
            create_data_loader(None)

    def test_filesystem_data_loader_missing_data_source_type(self):
        """Test FileSystemDataLoader with missing data_source_type."""
        config = {"connection_params": {"base_path": "/tmp/test"}}

        with pytest.raises(ValueError, match="data_source_type must be specified"):
            FileSystemDataLoader(config)

    def test_sql_data_loader_missing_data_source_type(self):
        """Test SQLDataLoader with missing data_source_type."""
        config = {"connection_params": {"connection_string": "sqlite:///test.db"}}

        with pytest.raises(ValueError, match="data_source_type must be specified"):
            SQLDataLoader(config)


class TestDataLoaderIntegration:
    """Test data loader integration with CLI."""

    def test_data_loader_creation_in_pipeline_context(self):
        """Test data loader creation as it would happen in a pipeline."""
        from cleared.transformers.pipelines import TablePipeline
        from cleared.config.structure import DeIDConfig, ClearedIOConfig

        # Create configuration similar to what CLI would create
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        # Create pipeline
        pipeline = TablePipeline("test_table", io_config.data, deid_config)

        # Test that pipeline can create data loader
        data_loader = pipeline._create_data_loader(io_config.data.input_config)

        # Verify data loader was created correctly
        assert isinstance(data_loader, FileSystemDataLoader)
        assert data_loader.data_source_type == "filesystem"

    def test_data_loader_creation_with_custom_configs(self):
        """Test data loader creation with custom configuration values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            io_config = IOConfig(
                io_type="filesystem",
                configs={
                    "base_path": temp_dir,
                    "file_format": "parquet",
                    "encoding": "utf-16",
                    "separator": "\t",
                },
            )

            data_loader = create_data_loader(io_config)

            # Verify custom values are preserved
            assert data_loader.base_path == Path(temp_dir)
            assert data_loader.file_format == "parquet"
            assert data_loader.encoding == "utf-16"
            assert data_loader.separator == "\t"

    def test_data_loader_creation_with_sql_config(self):
        """Test data loader creation with SQL configuration."""
        io_config = IOConfig(
            io_type="sql",
            configs={
                "connection_string": "postgresql://user:pass@localhost/db",
                "table_mappings": {
                    "patients": "patients_deid",
                    "encounters": "encounters_deid",
                },
                "validation_rules": {
                    "patients": {
                        "required_columns": ["patient_id", "age"],
                        "expected_types": {"patient_id": "int64", "age": "int64"},
                    }
                },
            },
        )

        with patch("cleared.io.sql.SQLDataLoader._initialize_connection"):
            data_loader = create_data_loader(io_config)

        # Verify SQL configuration
        assert isinstance(data_loader, SQLDataLoader)
        assert (
            data_loader.connection_params["connection_string"]
            == "postgresql://user:pass@localhost/db"
        )
        assert (
            data_loader.connection_params["table_mappings"]["patients"]
            == "patients_deid"
        )
        assert data_loader.connection_params["validation_rules"]["patients"][
            "required_columns"
        ] == ["patient_id", "age"]
