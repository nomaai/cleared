"""Unit tests for SQLDataLoader."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from omegaconf import DictConfig

# Check if SQLAlchemy is available
import importlib.util

if importlib.util.find_spec("sqlalchemy") is not None:
    from sqlalchemy.exc import SQLAlchemyError
    from cleared.io.sql import SQLDataLoader
    from cleared.io.base import IOConnectionError

    SQL_AVAILABLE = True
else:
    SQL_AVAILABLE = False
    SQLDataLoader = None

from cleared.io.base import TableNotFoundError, WriteError


def create_mock_sql_context_manager(mock_conn=None):
    """Create a properly mocked SQL context manager."""
    if mock_conn is None:
        mock_conn = Mock()

    mock_context_manager = Mock()
    mock_context_manager.__enter__ = Mock(return_value=mock_conn)
    mock_context_manager.__exit__ = Mock(return_value=None)
    return mock_context_manager


@pytest.mark.skipif(not SQL_AVAILABLE, reason="SQLAlchemy not available")
class TestSQLDataLoader:
    """Test the SQLDataLoader class."""

    def setup_method(self):
        """Set up test environment."""
        self.config = DictConfig(
            {
                "data_source_type": "sql",
                "connection_params": {
                    "database_url": "sqlite:///:memory:",
                    "echo": False,
                    "pool_pre_ping": True,
                    "pool_recycle": 3600,
                },
            }
        )

    def test_initialization_with_database_url(self):
        """Test initialization with database URL."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_conn.execute.return_value = None
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )

            loader = SQLDataLoader(self.config)

            assert loader.data_source_type == "sql"
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args
            assert call_args[0][0] == "sqlite:///:memory:"
            assert call_args[1]["echo"] is False
            assert call_args[1]["pool_pre_ping"] is True
            assert call_args[1]["pool_recycle"] == 3600

    def test_initialization_with_individual_params(self):
        """Test initialization with individual connection parameters."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            # Create SQLDataLoader with individual params
            config = DictConfig(
                {
                    "data_source_type": "sql",
                    "connection_params": {
                        "host": "localhost",
                        "port": 5432,
                        "database": "testdb",
                        "username": "user",
                        "password": "pass",
                        "driver": "postgresql",
                    },
                }
            )
            _ = SQLDataLoader(config)

            # Verify database URL was built correctly
            call_args = mock_create_engine.call_args
            assert call_args[0][0] == "postgresql://user:pass@localhost:5432/testdb"

    def test_initialization_connection_failure(self):
        """Test initialization fails when connection cannot be established."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection failure
            mock_engine.connect.side_effect = SQLAlchemyError("Connection failed")

            with pytest.raises(
                IOConnectionError, match="Failed to connect to database"
            ):
                SQLDataLoader(self.config)

    def test_build_database_url_missing_params(self):
        """Test building database URL with missing parameters."""
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

    def test_build_database_url_with_port(self):
        """Test building database URL with port."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            # Create SQLDataLoader with port
            config = DictConfig(
                {
                    "data_source_type": "sql",
                    "connection_params": {
                        "host": "localhost",
                        "port": 5432,
                        "database": "testdb",
                        "username": "user",
                        "password": "pass",
                        "driver": "postgresql",
                    },
                }
            )
            _ = SQLDataLoader(config)

            # Verify database URL includes port
            call_args = mock_create_engine.call_args
            assert call_args[0][0] == "postgresql://user:pass@localhost:5432/testdb"

    def test_build_database_url_without_port(self):
        """Test building database URL without port."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            # Create SQLDataLoader without port
            config = DictConfig(
                {
                    "data_source_type": "sql",
                    "connection_params": {
                        "host": "localhost",
                        "database": "testdb",
                        "username": "user",
                        "password": "pass",
                        "driver": "postgresql",
                    },
                }
            )
            _ = SQLDataLoader(config)

            # Verify database URL doesn't include port
            call_args = mock_create_engine.call_args
            assert call_args[0][0] == "postgresql://user:pass@localhost/testdb"

    def test_read_table_success(self):
        """Test reading table successfully."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            # Mock table_exists to return True
            with patch.object(loader, "table_exists", return_value=True):
                # Mock pd.read_sql
                expected_df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
                with patch(
                    "pandas.read_sql", return_value=expected_df
                ) as mock_read_sql:
                    result = loader.read_table("test_table")

                    pd.testing.assert_frame_equal(result, expected_df)
                    mock_read_sql.assert_called_once()
                    call_args = mock_read_sql.call_args
                    assert call_args[0][0] == "SELECT * FROM test_table"
                    assert call_args[0][1] == mock_conn

    def test_read_table_not_found(self):
        """Test reading non-existent table raises TableNotFoundError."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            # Mock table_exists to return False
            with patch.object(loader, "table_exists", return_value=False):
                with pytest.raises(
                    TableNotFoundError, match="Table 'test_table' does not exist"
                ):
                    loader.read_table("test_table")

    def test_read_table_connection_error(self):
        """Test reading table with connection error raises IOConnectionError."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            # Mock table_exists to return True
            with patch.object(loader, "table_exists", return_value=True):
                # Mock pd.read_sql to raise SQLAlchemyError
                with patch(
                    "pandas.read_sql", side_effect=SQLAlchemyError("Query failed")
                ):
                    with pytest.raises(
                        IOConnectionError, match="Failed to read table test_table"
                    ):
                        loader.read_table("test_table")

    def test_write_deid_table_success(self):
        """Test writing de-identified table successfully."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

            # Mock validate_data
            with patch.object(loader, "validate_data", return_value=True):
                # Mock df.to_sql
                with patch.object(test_data, "to_sql") as mock_to_sql:
                    loader.write_deid_table(test_data, "test_table")

                    mock_to_sql.assert_called_once_with(
                        "test_table",
                        mock_engine,
                        if_exists="replace",
                        index=False,
                        method="multi",
                    )

    def test_write_deid_table_with_parameters(self):
        """Test writing de-identified table with custom parameters."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

            # Mock validate_data
            with patch.object(loader, "validate_data", return_value=True):
                # Mock df.to_sql
                with patch.object(test_data, "to_sql") as mock_to_sql:
                    loader.write_deid_table(
                        test_data, "test_table", if_exists="append", index=True
                    )

                    mock_to_sql.assert_called_once_with(
                        "test_table",
                        mock_engine,
                        if_exists="append",
                        index=True,
                        method="multi",
                    )

    def test_write_deid_table_validation_error(self):
        """Test writing de-identified table with validation error."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

            # Mock validate_data to raise ValidationError
            with patch.object(
                loader, "validate_data", side_effect=Exception("Validation failed")
            ):
                with pytest.raises(
                    WriteError, match="Failed to write table test_table"
                ):
                    loader.write_deid_table(test_data, "test_table")

    def test_write_deid_table_sqlalchemy_error(self):
        """Test writing de-identified table with SQLAlchemy error."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

            # Mock validate_data
            with patch.object(loader, "validate_data", return_value=True):
                # Mock df.to_sql to raise SQLAlchemyError
                with patch.object(
                    test_data, "to_sql", side_effect=SQLAlchemyError("Write failed")
                ):
                    with pytest.raises(
                        WriteError, match="Failed to write table test_table"
                    ):
                        loader.write_deid_table(test_data, "test_table")

    def test_list_tables_success(self):
        """Test listing tables successfully."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            # Mock inspector
            mock_inspector = Mock()
            mock_inspector.get_table_names.return_value = ["table1", "table2", "table3"]

            with patch("sqlalchemy.inspect", return_value=mock_inspector):
                tables = loader.list_tables()

                assert tables == ["table1", "table2", "table3"]
                mock_inspector.get_table_names.assert_called_once()

    def test_list_tables_connection_error(self):
        """Test listing tables with connection error raises IOConnectionError."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            # Mock inspector to raise SQLAlchemyError
            with patch(
                "sqlalchemy.inspect", side_effect=SQLAlchemyError("Inspection failed")
            ):
                with pytest.raises(IOConnectionError, match="Failed to list tables"):
                    loader.list_tables()

    def test_table_exists_true(self):
        """Test table_exists returns True for existing table."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            # Mock inspector
            mock_inspector = Mock()
            mock_inspector.get_table_names.return_value = ["table1", "table2", "table3"]

            with patch("sqlalchemy.inspect", return_value=mock_inspector):
                assert loader.table_exists("table1") is True
                assert loader.table_exists("table2") is True
                assert loader.table_exists("table3") is True
                assert loader.table_exists("nonexistent") is False

    def test_table_exists_false_on_error(self):
        """Test table_exists returns False on SQLAlchemy error."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            # Mock inspector to raise SQLAlchemyError
            with patch(
                "sqlalchemy.inspect", side_effect=SQLAlchemyError("Inspection failed")
            ):
                assert loader.table_exists("any_table") is False

    def test_execute_query_success(self):
        """Test executing custom query successfully."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            expected_df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

            with patch("pandas.read_sql", return_value=expected_df) as mock_read_sql:
                result = loader.execute_query("SELECT * FROM test_table")

                pd.testing.assert_frame_equal(result, expected_df)
                mock_read_sql.assert_called_once_with(
                    "SELECT * FROM test_table", mock_conn
                )

    def test_execute_query_with_params(self):
        """Test executing query with parameters."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            expected_df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
            params = {"table_name": "test_table"}

            with patch("pandas.read_sql", return_value=expected_df) as mock_read_sql:
                result = loader.execute_query("SELECT * FROM :table_name", params)

                pd.testing.assert_frame_equal(result, expected_df)
                mock_read_sql.assert_called_once_with(
                    "SELECT * FROM :table_name", mock_conn, params=params
                )

    def test_execute_query_connection_error(self):
        """Test executing query with connection error raises ConnectionError."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            with patch("pandas.read_sql", side_effect=SQLAlchemyError("Query failed")):
                with pytest.raises(IOConnectionError, match="Failed to execute query"):
                    loader.execute_query("SELECT * FROM test_table")

    def test_create_table_success(self):
        """Test creating table successfully."""
        with (
            patch("cleared.io.sql.create_engine") as mock_create_engine,
            patch("cleared.io.sql.text") as mock_text,
        ):
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            mock_text.return_value = (
                "CREATE TABLE test_table (id INT, name VARCHAR(50))"
            )

            loader = SQLDataLoader(self.config)
            loader.create_table("test_table", "id INT, name VARCHAR(50)")

            # text should be called for both connection test and create table
            assert mock_text.call_count == 2
            mock_text.assert_any_call(
                "CREATE TABLE test_table (id INT, name VARCHAR(50))"
            )
            # execute should be called twice (connection test + create table)
            assert mock_conn.execute.call_count == 2
            mock_conn.commit.assert_called_once()

    def test_create_table_sqlalchemy_error(self):
        """Test creating table with SQLAlchemy error raises WriteError."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            # Mock text and connection execution to raise error
            with patch("sqlalchemy.text") as mock_text:
                mock_text.return_value = (
                    "CREATE TABLE test_table (id INT, name VARCHAR(50))"
                )
                mock_conn.execute.side_effect = SQLAlchemyError("Create failed")

                with pytest.raises(
                    WriteError, match="Failed to create table test_table"
                ):
                    loader.create_table("test_table", "id INT, name VARCHAR(50)")

    def test_drop_table_success(self):
        """Test dropping table successfully."""
        with (
            patch("cleared.io.sql.create_engine") as mock_create_engine,
            patch("cleared.io.sql.text") as mock_text,
        ):
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            mock_text.return_value = "DROP TABLE IF EXISTS test_table"

            loader = SQLDataLoader(self.config)
            loader.drop_table("test_table", if_exists=True)

            # text should be called for both connection test and drop table
            assert mock_text.call_count == 2
            mock_text.assert_any_call("DROP TABLE IF EXISTS test_table")
            # execute should be called twice (connection test + drop table)
            assert mock_conn.execute.call_count == 2
            mock_conn.commit.assert_called_once()

    def test_drop_table_without_if_exists(self):
        """Test dropping table without if_exists."""
        with (
            patch("cleared.io.sql.create_engine") as mock_create_engine,
            patch("cleared.io.sql.text") as mock_text,
        ):
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            mock_text.return_value = "DROP TABLE test_table"

            loader = SQLDataLoader(self.config)
            loader.drop_table("test_table", if_exists=False)

            # text should be called for both connection test and drop table
            assert mock_text.call_count == 2
            mock_text.assert_any_call("DROP TABLE test_table")
            # execute should be called twice (connection test + drop table)
            assert mock_conn.execute.call_count == 2
            mock_conn.commit.assert_called_once()

    def test_drop_table_sqlalchemy_error(self):
        """Test dropping table with SQLAlchemy error raises WriteError."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            # Mock text and connection execution to raise error
            with patch("sqlalchemy.text") as mock_text:
                mock_text.return_value = "DROP TABLE test_table"
                mock_conn.execute.side_effect = SQLAlchemyError("Drop failed")

                with pytest.raises(WriteError, match="Failed to drop table test_table"):
                    loader.drop_table("test_table")

    def test_close_connection(self):
        """Test closing connection."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)
            loader.close_connection()

            mock_engine.dispose.assert_called_once()

    def test_close_connection_no_engine(self):
        """Test closing connection when engine doesn't exist."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            # Remove engine attribute
            del loader.engine

            # Should not raise exception
            loader.close_connection()

    def test_context_manager(self):
        """Test context manager functionality."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            with SQLDataLoader(self.config) as loader:
                assert loader.data_source_type == "sql"
                assert loader.engine == mock_engine

            # Engine should be disposed after context exit
            mock_engine.dispose.assert_called_once()

    def test_repr(self):
        """Test string representation."""
        with patch("cleared.io.sql.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            # Mock connection test
            mock_conn = Mock()
            mock_engine.connect.return_value = create_mock_sql_context_manager(
                mock_conn
            )
            mock_conn.execute.return_value = None

            loader = SQLDataLoader(self.config)

            expected = "SQLDataLoader(data_source_type='sql')"
            assert repr(loader) == expected


@pytest.mark.skipif(not SQL_AVAILABLE, reason="SQLAlchemy not available")
class TestSQLDataLoaderIntegration:
    """Integration tests for SQLDataLoader."""

    def test_full_workflow_sqlite(self):
        """Test complete SQLite workflow using in-memory database."""
        config = DictConfig(
            {
                "data_source_type": "sql",
                "connection_params": {"database_url": "sqlite:///:memory:"},
                "table_mappings": {"patients": "patients_deid"},
                "validation_rules": {
                    "patients": {
                        "required_columns": ["id", "name"],
                        "expected_types": {"id": "int64"},
                    }
                },
            }
        )

        with SQLDataLoader(config) as loader:
            # Create test data
            test_data = pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                    "age": [25, 30, 35],
                }
            )

            # Create table
            loader.create_table("patients", "id INTEGER, name VARCHAR(50), age INTEGER")

            # Write data
            loader.write_deid_table(test_data, "patients")

            # Read data back
            read_data = loader.read_table("patients")
            pd.testing.assert_frame_equal(read_data, test_data)

            # Validate data
            assert loader.validate_data(read_data, "patients") is True

            # Test table mapping
            original, deid = loader.get_table_mapping("patients")
            assert original == "patients"
            assert deid == "patients_deid"

            # List tables
            tables = loader.list_tables()
            assert "patients" in tables

            # Test table exists
            assert loader.table_exists("patients") is True
            assert loader.table_exists("nonexistent") is False

            # Test custom query
            query_result = loader.execute_query(
                "SELECT COUNT(*) as count FROM patients"
            )
            assert query_result.iloc[0]["count"] == 3

            # Test drop table
            loader.drop_table("patients")
            assert loader.table_exists("patients") is False

    def test_append_workflow(self):
        """Test append workflow with multiple writes."""
        config = DictConfig(
            {
                "data_source_type": "sql",
                "connection_params": {"database_url": "sqlite:///:memory:"},
            }
        )

        with SQLDataLoader(config) as loader:
            # Create table
            loader.create_table("test_table", "id INTEGER, name VARCHAR(50)")

            # Initial data
            initial_data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
            loader.write_deid_table(initial_data, "test_table", if_exists="replace")

            # Append more data
            additional_data = pd.DataFrame({"id": [3, 4], "name": ["C", "D"]})
            loader.write_deid_table(additional_data, "test_table", if_exists="append")

            # Read final data
            final_data = loader.read_table("test_table")
            expected_data = pd.concat(
                [initial_data, additional_data], ignore_index=True
            )

            pd.testing.assert_frame_equal(final_data, expected_data)

    def test_large_dataset(self):
        """Test handling large dataset."""
        config = DictConfig(
            {
                "data_source_type": "sql",
                "connection_params": {"database_url": "sqlite:///:memory:"},
            }
        )

        with SQLDataLoader(config) as loader:
            # Create table
            loader.create_table(
                "large_table", "id INTEGER, name VARCHAR(50), value REAL"
            )

            # Create large dataset
            large_data = pd.DataFrame(
                {
                    "id": range(1000),
                    "name": [f"Person_{i}" for i in range(1000)],
                    "value": [i * 0.1 for i in range(1000)],
                }
            )

            # Write large dataset
            loader.write_deid_table(large_data, "large_table")

            # Read large dataset
            read_data = loader.read_table("large_table")

            assert len(read_data) == 1000
            pd.testing.assert_frame_equal(read_data, large_data)

    def test_parameterized_query(self):
        """Test parameterized query execution."""
        config = DictConfig(
            {
                "data_source_type": "sql",
                "connection_params": {"database_url": "sqlite:///:memory:"},
            }
        )

        with SQLDataLoader(config) as loader:
            # Create table
            loader.create_table(
                "test_table", "id INTEGER, name VARCHAR(50), age INTEGER"
            )

            # Insert test data
            test_data = pd.DataFrame(
                {
                    "id": [1, 2, 3, 4, 5],
                    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                    "age": [25, 30, 35, 40, 45],
                }
            )
            loader.write_deid_table(test_data, "test_table")

            # Test parameterized query
            query = "SELECT * FROM test_table WHERE age >= :min_age AND age <= :max_age"
            params = {"min_age": 30, "max_age": 40}

            result = loader.execute_query(query, params)

            # Should return Bob, Charlie, and David (ages 30, 35, and 40)
            assert len(result) == 3
            assert set(result["name"].tolist()) == {"Bob", "Charlie", "David"}

    def test_error_handling_workflow(self):
        """Test error handling in various scenarios."""
        config = DictConfig(
            {
                "data_source_type": "sql",
                "connection_params": {"database_url": "sqlite:///:memory:"},
            }
        )

        with SQLDataLoader(config) as loader:
            # Test reading non-existent table
            with pytest.raises(TableNotFoundError):
                loader.read_table("nonexistent_table")

            # Test writing to non-existent table (should succeed and create table)
            test_data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
            loader.write_deid_table(test_data, "nonexistent_table")

            # Verify table was created
            assert loader.table_exists("nonexistent_table")

            # Test invalid query
            with pytest.raises(IOConnectionError):
                loader.execute_query("INVALID SQL QUERY")

            # Test dropping non-existent table with if_exists=False
            with pytest.raises(WriteError):
                loader.drop_table("truly_nonexistent_table", if_exists=False)

            # Test dropping non-existent table with if_exists=True (should succeed)
            loader.drop_table("nonexistent_table", if_exists=True)

    def test_table_mapping_functionality(self):
        """Test table mapping functionality in SQLDataLoader."""
        config = DictConfig(
            {
                "data_source_type": "sql",
                "connection_params": {"database_url": "sqlite:///:memory:"},
                "table_mappings": {
                    "patients": "patients_deid",
                    "encounters": "encounters_deid",
                },
                "suffix": "_deid",
            }
        )

        with SQLDataLoader(config) as loader:
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
                "data_source_type": "sql",
                "connection_params": {"database_url": "sqlite:///:memory:"},
                "table_mappings": {"patients": "patients_deid"},
                "suffix": "_deid",
            }
        )

        with SQLDataLoader(config) as loader:
            # Create test tables
            test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

            # Create original table
            loader.write_deid_table(test_data, "patients")

            # Create deid table
            loader.write_deid_table(test_data, "patients_deid")

            # Create other table
            loader.write_deid_table(test_data, "other_table")

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
                "data_source_type": "sql",
                "connection_params": {"database_url": "sqlite:///:memory:"},
                "table_mappings": {"patients": "patients_deid"},
                "suffix": "_deid",
            }
        )

        with SQLDataLoader(config) as loader:
            # Create test tables
            test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

            # Create original table
            loader.write_deid_table(test_data, "patients")

            # Create deid table
            loader.write_deid_table(test_data, "patients_deid")

            # Create other table with suffix
            loader.write_deid_table(test_data, "other_table_deid")

            # Create regular table
            loader.write_deid_table(test_data, "regular_table")

            # Test list_deid_tables
            deid_tables = loader.list_deid_tables()
            assert "patients_deid" in deid_tables  # Mapped table
            assert "other_table_deid" in deid_tables  # Suffixed table
            assert "patients" not in deid_tables  # Original table
            assert "regular_table" not in deid_tables  # Regular table without suffix
