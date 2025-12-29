"""
SQL database based data loader implementation.

This module provides a concrete implementation of BaseDataLoader for
SQL database data sources (PostgreSQL, MySQL, SQLite, etc.).
"""

from __future__ import annotations

from typing import Any
from pathlib import Path
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .base import BaseDataLoader, IOConnectionError, TableNotFoundError, WriteError


class SQLDataLoader(BaseDataLoader):
    """
    Data loader for SQL database data sources.

    This loader handles reading from and writing to various SQL databases
    using SQLAlchemy for database abstraction.

    Supported databases:
        - PostgreSQL
        - MySQL
        - SQLite
        - SQL Server
        - Oracle

    Configuration example:
        data_source_type: sql
        connection_params:
            database_url: "postgresql://user:pass@localhost:5432/dbname"
            # OR individual parameters:
            # driver: "postgresql"
            # host: "localhost"
            # port: 5432
            # username: "user"
            # password: "pass"
            # database: "dbname"
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
        Initialize SQL database connection.

        Raises:
            IOConnectionError: If connection cannot be established

        """
        try:
            # Build database URL if individual parameters are provided
            if "database_url" in self.connection_params:
                database_url = self.connection_params["database_url"]
            else:
                database_url = self._build_database_url()

            # Create SQLAlchemy engine
            self.engine = create_engine(
                database_url,
                echo=self.connection_params.get("echo", False),
                pool_pre_ping=self.connection_params.get("pool_pre_ping", True),
                pool_recycle=self.connection_params.get("pool_recycle", 3600),
            )

            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

        except SQLAlchemyError as e:
            raise IOConnectionError(f"Failed to connect to database: {e!s}") from e

    def _build_database_url(self) -> str:
        """
        Build database URL from individual parameters.

        Returns:
            Database URL string

        Raises:
            ValueError: If required parameters are missing

        """
        required_params = ["driver", "host", "username", "password", "database"]
        missing_params = [p for p in required_params if p not in self.connection_params]

        if missing_params:
            raise ValueError(
                f"Missing required connection parameters: {missing_params}"
            )

        driver = self.connection_params["driver"]
        host = self.connection_params["host"]
        port = self.connection_params.get("port", "")
        username = self.connection_params["username"]
        password = self.connection_params["password"]
        database = self.connection_params["database"]

        if port:
            port = f":{port}"

        return f"{driver}://{username}:{password}@{host}{port}/{database}"

    def get_table_paths(self, table_name: str) -> Path | list[Path]:
        """
        Get path(s) for a table.

        For SQL databases, segments are not supported. This method returns
        a Path representation of the table name if it exists.

        Args:
            table_name: Name of the table

        Returns:
            Path representation of the table name (single table only)

        Raises:
            TableNotFoundError: If table doesn't exist

        """
        if not self.table_exists(table_name):
            raise TableNotFoundError(f"Table '{table_name}' does not exist")
        # Return a Path-like representation (table name as path)
        return Path(table_name)

    def read_table(
        self,
        table_name: str,
        rows_limit: int | None = None,
        segment_path: Path | None = None,
    ) -> pd.DataFrame:
        """
        Read data from a SQL table.

        Args:
            table_name: Name of the table to read from
            rows_limit: Optional limit on number of rows to read (for testing)
            segment_path: Not supported for SQL (ignored)

        Returns:
            DataFrame containing the table data

        Raises:
            TableNotFoundError: If the table doesn't exist
            IOConnectionError: If database query fails

        """
        try:
            # Check if table exists
            if not self.table_exists(table_name):
                raise TableNotFoundError(f"Table '{table_name}' does not exist")

            # segment_path is not supported for SQL databases
            if segment_path is not None:
                raise ValueError("segment_path is not supported for SQL data loaders")

            # Build query to read table (with optional limit)
            if rows_limit is not None:
                # Use database-specific LIMIT syntax
                query = f"SELECT * FROM {table_name} LIMIT {rows_limit}"
            else:
                query = f"SELECT * FROM {table_name}"

            # Execute query
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)

            return df

        except SQLAlchemyError as e:
            raise IOConnectionError(f"Failed to read table {table_name}: {e!s}") from e

    def write_deid_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False,
        segment_name: str | None = None,
    ) -> None:
        """
        Write de-identified data to a SQL table.

        Args:
            df: DataFrame containing the de-identified data
            table_name: Name of the table to write to
            if_exists: How to behave if table exists ('replace', 'append', 'fail')
            index: Whether to write DataFrame index as a column
            segment_name: Not supported for SQL (ignored)

        Raises:
            WriteError: If writing fails

        """
        # segment_name is not supported for SQL databases
        if segment_name is not None:
            raise ValueError("segment_name is not supported for SQL data loaders")
        try:
            # Validate data before writing
            self.validate_data(df, table_name)

            # Write to database
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=index,
                method="multi",  # Use multi-row insert for better performance
            )

        except SQLAlchemyError as e:
            raise WriteError(f"Failed to write table {table_name}: {e!s}") from e
        except Exception as e:
            raise WriteError(f"Failed to write table {table_name}: {e!s}") from e

    def list_tables(self) -> list[str]:
        """
        List available tables in the database.

        Returns:
            List of table names

        Raises:
            IOConnectionError: If database query fails

        """
        try:
            with self.engine.connect():
                # Get table names based on database type
                inspector = sa.inspect(self.engine)
                tables = inspector.get_table_names()
                return sorted(tables)

        except SQLAlchemyError as e:
            raise IOConnectionError(f"Failed to list tables: {e!s}") from e

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

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists, False otherwise

        """
        try:
            with self.engine.connect():
                inspector = sa.inspect(self.engine)
                return table_name in inspector.get_table_names()
        except SQLAlchemyError:
            return False

    def execute_query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        """
        Execute a custom SQL query.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            DataFrame containing query results

        Raises:
            IOConnectionError: If query execution fails

        """
        try:
            with self.engine.connect() as conn:
                if params:
                    df = pd.read_sql(query, conn, params=params)
                else:
                    df = pd.read_sql(query, conn)
                return df
        except SQLAlchemyError as e:
            raise IOConnectionError(f"Failed to execute query: {e!s}") from e

    def create_table(self, table_name: str, schema: str) -> None:
        """
        Create a table with the specified schema.

        Args:
            table_name: Name of the table to create
            schema: SQL DDL schema definition

        Raises:
            WriteError: If table creation fails

        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE TABLE {table_name} ({schema})"))
                conn.commit()
        except SQLAlchemyError as e:
            raise WriteError(f"Failed to create table {table_name}: {e!s}") from e

    def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """
        Drop a table.

        Args:
            table_name: Name of the table to drop
            if_exists: Whether to ignore error if table doesn't exist

        Raises:
            WriteError: If table dropping fails

        """
        try:
            with self.engine.connect() as conn:
                if if_exists:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                else:
                    conn.execute(text(f"DROP TABLE {table_name}"))
                conn.commit()
        except SQLAlchemyError as e:
            raise WriteError(f"Failed to drop table {table_name}: {e!s}") from e

    def close_connection(self) -> None:
        """Close database connection."""
        if hasattr(self, "engine"):
            self.engine.dispose()
