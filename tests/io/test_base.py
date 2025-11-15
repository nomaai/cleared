"""Unit tests for BaseDataLoader and related classes."""

import pytest
import pandas as pd
from unittest.mock import patch
from omegaconf import DictConfig, OmegaConf

from cleared.io.base import (
    BaseDataLoader,
    DataLoaderError,
    IOConnectionError,
    TableNotFoundError,
    WriteError,
    ValidationError,
)


class ConcreteDataLoader(BaseDataLoader):
    """Concrete implementation of BaseDataLoader for testing."""

    def _initialize_connection(self) -> None:
        """Mock implementation of connection initialization."""
        self.connection_initialized = True

    def read_table(
        self, table_name: str, rows_limit: int | None = None
    ) -> pd.DataFrame:
        """Mock implementation of read_table."""
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        if rows_limit is not None:
            df = df.head(rows_limit)
        return df

    def write_deid_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False,
    ) -> None:
        """Mock implementation of write_deid_table."""
        self.written_data = df
        self.written_table = table_name


class CustomDataLoader(BaseDataLoader):
    """Custom data loader example for testing."""

    def _initialize_connection(self) -> None:
        """Initialize custom connection."""
        self.data = {}  # In-memory storage for example

    def read_table(
        self, table_name: str, rows_limit: int | None = None
    ) -> pd.DataFrame:
        """Read from custom data source."""
        if table_name not in self.data:
            raise TableNotFoundError(f"Table {table_name} not found")

        df = self.data[table_name]
        if rows_limit is not None:
            df = df.head(rows_limit)
        return df

    def write_deid_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False,
    ) -> None:
        """Write to custom data source."""
        if if_exists == "fail" and table_name in self.data:
            raise WriteError(f"Table {table_name} already exists")
        elif if_exists == "append" and table_name in self.data:
            # Append to existing data
            existing_df = self.data[table_name]
            self.data[table_name] = pd.concat([existing_df, df], ignore_index=True)
        else:
            # Replace or create new
            self.data[table_name] = df.copy()

    def list_tables(self) -> list[str]:
        """List available tables in custom data source."""
        return sorted(self.data.keys())


class TestBaseDataLoader:
    """Test the BaseDataLoader abstract base class."""

    def test_initialization_with_valid_config(self):
        """Test initialization with valid configuration."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "connection_params": {"host": "localhost", "port": 5432},
                "table_mappings": {"patients": "patients_deid"},
                "validation_rules": {"patients": {"required_columns": ["id"]}},
            }
        )

        loader = ConcreteDataLoader(config)

        assert loader.data_source_type == "test"
        assert loader.connection_params == {"host": "localhost", "port": 5432}
        assert loader.table_mappings == {"patients": "patients_deid"}
        assert loader.validation_rules == {"patients": {"required_columns": ["id"]}}
        assert loader.connection_initialized is True

    def test_initialization_missing_data_source_type(self):
        """Test initialization fails when data_source_type is missing."""
        config = DictConfig({"connection_params": {"host": "localhost"}})

        with pytest.raises(ValueError, match="data_source_type must be specified"):
            ConcreteDataLoader(config)

    def test_initialization_with_minimal_config(self):
        """Test initialization with minimal configuration."""
        config = DictConfig({"data_source_type": "test"})

        loader = ConcreteDataLoader(config)

        assert loader.data_source_type == "test"
        assert loader.connection_params == {}
        assert loader.table_mappings == {}
        assert loader.validation_rules == {}

    def test_extract_data_source_type(self):
        """Test data source type extraction."""
        config = DictConfig({"data_source_type": "postgresql"})
        loader = ConcreteDataLoader(config)

        assert loader._extract_data_source_type() == "postgresql"

    def test_extract_connection_params(self):
        """Test connection parameters extraction."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "connection_params": {"host": "localhost", "port": 5432},
            }
        )
        loader = ConcreteDataLoader(config)

        assert loader._extract_connection_params() == {
            "host": "localhost",
            "port": 5432,
        }

    def test_extract_connection_params_missing(self):
        """Test connection parameters extraction when missing."""
        config = DictConfig({"data_source_type": "test"})
        loader = ConcreteDataLoader(config)

        assert loader._extract_connection_params() == {}

    def test_extract_table_mappings(self):
        """Test table mappings extraction."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "table_mappings": {
                    "patients": "patients_deid",
                    "encounters": "encounters_deid",
                },
            }
        )
        loader = ConcreteDataLoader(config)

        assert loader._extract_table_mappings() == {
            "patients": "patients_deid",
            "encounters": "encounters_deid",
        }

    def test_extract_validation_rules(self):
        """Test validation rules extraction."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "validation_rules": {
                    "patients": {"required_columns": ["id", "name"]},
                    "encounters": {"expected_types": {"id": "int64"}},
                },
            }
        )
        loader = ConcreteDataLoader(config)

        expected_rules = {
            "patients": {"required_columns": ["id", "name"]},
            "encounters": {"expected_types": {"id": "int64"}},
        }
        assert loader._extract_validation_rules() == expected_rules

    def test_get_table_mapping_existing(self):
        """Test getting table mapping for existing table."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "table_mappings": {"patients": "patients_deid"},
            }
        )
        loader = ConcreteDataLoader(config)

        original, deid = loader.get_table_mapping("patients")
        assert original == "patients"
        assert deid == "patients_deid"

    def test_get_table_mapping_nonexistent(self):
        """Test getting table mapping for non-existent table."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "table_mappings": {"patients": "patients_deid"},
                "suffix": "_deid",
            }
        )
        loader = ConcreteDataLoader(config)

        original, deid = loader.get_table_mapping("encounters")
        assert original == "encounters"
        assert deid == "encounters_deid"

    def test_validate_data_no_rules(self):
        """Test data validation with no rules."""
        config = DictConfig({"data_source_type": "test"})
        loader = ConcreteDataLoader(config)

        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        assert loader.validate_data(df, "test_table") is True

    def test_validate_data_required_columns_pass(self):
        """Test data validation with required columns that pass."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "validation_rules": {
                    "test_table": {"required_columns": ["id", "name"]}
                },
            }
        )
        loader = ConcreteDataLoader(config)

        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        assert loader.validate_data(df, "test_table") is True

    def test_validate_data_required_columns_fail(self):
        """Test data validation with missing required columns."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "validation_rules": {
                    "test_table": {"required_columns": ["id", "name", "age"]}
                },
            }
        )
        loader = ConcreteDataLoader(config)

        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

        with pytest.raises(ValueError, match="Missing required columns"):
            loader.validate_data(df, "test_table")

    def test_validate_data_expected_types_pass(self):
        """Test data validation with expected types that pass."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "validation_rules": {
                    "test_table": {"expected_types": {"id": "int64", "name": "object"}}
                },
            }
        )
        loader = ConcreteDataLoader(config)

        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        assert loader.validate_data(df, "test_table") is True

    def test_validate_data_expected_types_fail(self):
        """Test data validation with wrong expected types."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "validation_rules": {
                    "test_table": {"expected_types": {"id": "float64"}}
                },
            }
        )
        loader = ConcreteDataLoader(config)

        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

        with pytest.raises(ValueError, match="has type 'int64', expected 'float64'"):
            loader.validate_data(df, "test_table")

    def test_validate_data_missing_column_in_expected_types(self):
        """Test data validation when column in expected_types doesn't exist."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "validation_rules": {
                    "test_table": {"expected_types": {"nonexistent": "int64"}}
                },
            }
        )
        loader = ConcreteDataLoader(config)

        df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        assert loader.validate_data(df, "test_table") is True

    def test_table_exists_with_list_tables_implementation(self):
        """Test table_exists when list_tables is implemented."""
        config = DictConfig({"data_source_type": "test"})
        loader = ConcreteDataLoader(config)

        # Mock list_tables to return specific tables
        with patch.object(loader, "list_tables", return_value=["table1", "table2"]):
            assert loader.table_exists("table1") is True
            assert loader.table_exists("table3") is False

    def test_table_exists_fallback_to_read_table(self):
        """Test table_exists fallback when list_tables is not implemented."""
        config = DictConfig({"data_source_type": "test"})
        loader = ConcreteDataLoader(config)

        # Mock list_tables to raise NotImplementedError
        with patch.object(loader, "list_tables", side_effect=NotImplementedError):
            with patch.object(loader, "read_table") as mock_read:
                # Test existing table
                mock_read.return_value = pd.DataFrame()
                assert loader.table_exists("existing_table") is True

                # Test non-existing table
                mock_read.side_effect = Exception()
                assert loader.table_exists("nonexistent_table") is False

    def test_context_manager(self):
        """Test context manager functionality."""
        config = DictConfig({"data_source_type": "test"})

        with ConcreteDataLoader(config) as loader:
            assert loader.connection_initialized is True

        # Connection should be closed after context exit
        # (In this case, close_connection is a no-op, but the pattern is tested)

    def test_repr(self):
        """Test string representation."""
        config = DictConfig({"data_source_type": "postgresql"})
        loader = ConcreteDataLoader(config)

        expected = "ConcreteDataLoader(data_source_type='postgresql')"
        assert repr(loader) == expected

    def test_close_connection_no_op(self):
        """Test close_connection default implementation."""
        config = DictConfig({"data_source_type": "test"})
        loader = ConcreteDataLoader(config)

        # Should not raise any exception
        loader.close_connection()


class TestDataLoaderExceptions:
    """Test data loader exception classes."""

    def test_data_loader_error_inheritance(self):
        """Test DataLoaderError inheritance."""
        error = DataLoaderError("Test error")
        assert isinstance(error, Exception)
        assert str(error) == "Test error"

    def test_connection_error_inheritance(self):
        """Test IOConnectionError inheritance."""
        error = IOConnectionError("Connection failed")
        assert isinstance(error, DataLoaderError)
        assert str(error) == "Connection failed"

    def test_table_not_found_error_inheritance(self):
        """Test TableNotFoundError inheritance."""
        error = TableNotFoundError("Table not found")
        assert isinstance(error, DataLoaderError)
        assert str(error) == "Table not found"

    def test_write_error_inheritance(self):
        """Test WriteError inheritance."""
        error = WriteError("Write failed")
        assert isinstance(error, DataLoaderError)
        assert str(error) == "Write failed"

    def test_validation_error_inheritance(self):
        """Test ValidationError inheritance."""
        error = ValidationError("Validation failed")
        assert isinstance(error, DataLoaderError)
        assert str(error) == "Validation failed"

    def test_exception_chaining(self):
        """Test exception chaining."""
        original_error = ValueError("Original error")
        try:
            raise DataLoaderError("Data error") from original_error
        except DataLoaderError as data_error:
            assert data_error.__cause__ is original_error
            assert isinstance(data_error.__cause__, ValueError)


class TestBaseDataLoaderIntegration:
    """Integration tests for BaseDataLoader."""

    def test_full_workflow(self):
        """Test complete data loader workflow."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "connection_params": {"host": "localhost"},
                "table_mappings": {"patients": "patients_deid"},
                "validation_rules": {
                    "patients": {
                        "required_columns": ["id", "name"],
                        "expected_types": {"id": "int64"},
                    }
                },
            }
        )

        loader = ConcreteDataLoader(config)

        # Test reading data
        df = loader.read_table("patients")
        assert len(df) == 3
        assert list(df.columns) == ["id", "name"]

        # Test validation
        assert loader.validate_data(df, "patients") is True

        # Test writing data
        loader.write_deid_table(df, "patients_deid")
        assert loader.written_table == "patients_deid"
        assert loader.written_data.equals(df)

        # Test table mapping
        original, deid = loader.get_table_mapping("patients")
        assert original == "patients"
        assert deid == "patients_deid"

    def test_config_from_yaml_string(self):
        """Test initialization with YAML configuration string."""
        yaml_config = """
        data_source_type: test
        connection_params:
          host: localhost
          port: 5432
        table_mappings:
          patients: patients_deid
        validation_rules:
          patients:
            required_columns: [id, name]
        """

        config = OmegaConf.create(yaml_config)
        loader = ConcreteDataLoader(config)

        assert loader.data_source_type == "test"
        assert loader.connection_params["host"] == "localhost"
        assert loader.connection_params["port"] == 5432
        assert loader.table_mappings["patients"] == "patients_deid"

    def test_nested_config_access(self):
        """Test accessing nested configuration values."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "connection_params": {
                    "database": {
                        "host": "localhost",
                        "credentials": {"username": "user", "password": "pass"},
                    }
                },
            }
        )

        loader = ConcreteDataLoader(config)

        # Test accessing nested values
        assert loader.connection_params["database"]["host"] == "localhost"
        assert loader.connection_params["database"]["credentials"]["username"] == "user"

    def test_missing_optional_config_sections(self):
        """Test behavior when optional config sections are missing."""
        config = DictConfig({"data_source_type": "test"})

        loader = ConcreteDataLoader(config)

        # All optional sections should default to empty dicts
        assert loader.connection_params == {}
        assert loader.table_mappings == {}
        assert loader.validation_rules == {}

        # Methods should still work
        original, deid = loader.get_table_mapping("test")
        assert original == "test"
        assert deid == "test"  # No suffix configured, so should return same name
        assert loader.validate_data(pd.DataFrame(), "test") is True

    def test_table_mapping_functionality(self):
        """Test table mapping functionality."""
        config = DictConfig(
            {
                "data_source_type": "test",
                "connection_params": {"host": "localhost"},
                "table_mappings": {
                    "patients": "patients_deid",
                    "encounters": "encounters_deid",
                },
                "suffix": "_deid",
            }
        )

        loader = ConcreteDataLoader(config)

        # Test get_deid_table_name with mapping
        assert loader.get_deid_table_name("patients") == "patients_deid"
        assert loader.get_deid_table_name("encounters") == "encounters_deid"

        # Test get_deid_table_name with suffix fallback
        assert loader.get_deid_table_name("other_table") == "other_table_deid"

        # Test get_original_table_name
        assert loader.get_original_table_name("patients_deid") == "patients"
        assert loader.get_original_table_name("encounters_deid") == "encounters"
        assert loader.get_original_table_name("other_table_deid") == "other_table"
        assert loader.get_original_table_name("unknown_table") == "unknown_table"

        # Test get_table_mapping
        original, deid = loader.get_table_mapping("patients")
        assert original == "patients"
        assert deid == "patients_deid"

        original, deid = loader.get_table_mapping("patients_deid")
        assert original == "patients"
        assert deid == "patients_deid"

        original, deid = loader.get_table_mapping("other_table")
        assert original == "other_table"
        assert deid == "other_table_deid"

    def test_custom_loader_example(self):
        """Test the custom loader example from the documentation."""
        config = DictConfig(
            {
                "data_source_type": "custom",
                "connection_params": {"custom_param": "value"},
                "suffix": "_deid",
            }
        )

        # Test custom loader initialization
        with CustomDataLoader(config) as loader:
            assert loader.data_source_type == "custom"
            assert loader.connection_params == {"custom_param": "value"}
            assert loader.suffix == "_deid"

            # Create some sample data
            sample_df = pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )

            # Test write_deid_table
            loader.write_deid_table(sample_df, "users")
            assert "users" in loader.data
            pd.testing.assert_frame_equal(loader.data["users"], sample_df)

            # Test read_table
            df = loader.read_table("users")
            pd.testing.assert_frame_equal(df, sample_df)

            # Test list_tables
            tables = loader.list_tables()
            assert "users" in tables

            # Test table mapping
            deid_name = loader.get_deid_table_name("users")
            assert deid_name == "users_deid"

            # Test write with different if_exists modes
            new_data = pd.DataFrame({"id": [4], "name": ["David"], "age": [40]})

            # Test append mode
            loader.write_deid_table(new_data, "users", if_exists="append")
            combined_df = loader.read_table("users")
            assert len(combined_df) == 4

            # Test fail mode
            with pytest.raises(WriteError, match="Table users already exists"):
                loader.write_deid_table(new_data, "users", if_exists="fail")

            # Test replace mode
            loader.write_deid_table(new_data, "users", if_exists="replace")
            replaced_df = loader.read_table("users")
            assert len(replaced_df) == 1
            pd.testing.assert_frame_equal(replaced_df, new_data)

    def test_custom_loader_table_not_found(self):
        """Test custom loader raises TableNotFoundError for non-existent table."""
        config = DictConfig({"data_source_type": "custom", "connection_params": {}})

        with CustomDataLoader(config) as loader:
            with pytest.raises(TableNotFoundError, match="Table nonexistent not found"):
                loader.read_table("nonexistent")

    def test_custom_loader_context_manager(self):
        """Test custom loader context manager functionality."""
        config = DictConfig({"data_source_type": "custom", "connection_params": {}})

        # Test context manager
        with CustomDataLoader(config) as loader:
            assert loader.data_source_type == "custom"
            # Context manager should work without errors
            loader.write_deid_table(pd.DataFrame({"id": [1]}), "test")

        # After context exit, loader should still be usable
        assert loader.data_source_type == "custom"
