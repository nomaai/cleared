"""Unit tests for verify utility functions."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from cleared.cli.cmds.verify.utils import (
    get_column_dropper_columns,
    load_data_for_table,
    print_verification_results,
)
from cleared.cli.cmds.verify.model import (
    ColumnComparisonResult,
    TableVerificationResult,
    VerificationOverview,
    VerificationResult,
)
from cleared.config.structure import (
    ClearedConfig,
    ClearedIOConfig,
    DeIDConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
)


class TestGetColumnDropperColumns:
    """Test get_column_dropper_columns function."""

    def test_no_column_droppers(self):
        """Test when no ColumnDropper transformers exist."""
        config = self._create_test_config()
        config.tables["table1"].transformers = [
            TransformerConfig(method="IDDeidentifier", configs={}),
        ]

        result = get_column_dropper_columns(config, "table1")

        assert result == set()

    def test_single_column_dropper(self):
        """Test with single ColumnDropper."""
        config = self._create_test_config()
        config.tables["table1"].transformers = [
            TransformerConfig(
                method="ColumnDropper",
                configs={"idconfig": {"name": "col1"}},
            ),
        ]

        result = get_column_dropper_columns(config, "table1")

        assert result == {"col1"}

    def test_multiple_column_droppers(self):
        """Test with multiple ColumnDroppers."""
        config = self._create_test_config()
        config.tables["table1"].transformers = [
            TransformerConfig(
                method="ColumnDropper",
                configs={"idconfig": {"name": "col1"}},
            ),
            TransformerConfig(
                method="ColumnDropper",
                configs={"idconfig": {"name": "col2"}},
            ),
        ]

        result = get_column_dropper_columns(config, "table1")

        assert result == {"col1", "col2"}

    def test_mixed_transformers(self):
        """Test with mixed transformers."""
        config = self._create_test_config()
        config.tables["table1"].transformers = [
            TransformerConfig(method="IDDeidentifier", configs={}),
            TransformerConfig(
                method="ColumnDropper",
                configs={"idconfig": {"name": "col1"}},
            ),
            TransformerConfig(method="DateTimeDeidentifier", configs={}),
        ]

        result = get_column_dropper_columns(config, "table1")

        assert result == {"col1"}

    def test_table_not_in_config(self):
        """Test when table doesn't exist in config."""
        config = self._create_test_config()

        result = get_column_dropper_columns(config, "nonexistent_table")

        assert result == set()

    def test_missing_idconfig(self):
        """Test when ColumnDropper has no idconfig."""
        config = self._create_test_config()
        config.tables["table1"].transformers = [
            TransformerConfig(method="ColumnDropper", configs={}),
        ]

        result = get_column_dropper_columns(config, "table1")

        assert result == set()

    def test_idconfig_not_dict(self):
        """Test when idconfig is not a dict."""
        config = self._create_test_config()
        config.tables["table1"].transformers = [
            TransformerConfig(
                method="ColumnDropper",
                configs={"idconfig": "not_a_dict"},
            ),
        ]

        result = get_column_dropper_columns(config, "table1")

        assert result == set()

    def test_idconfig_missing_name(self):
        """Test when idconfig dict has no name key."""
        config = self._create_test_config()
        config.tables["table1"].transformers = [
            TransformerConfig(
                method="ColumnDropper",
                configs={"idconfig": {"uid": "test"}},
            ),
        ]

        result = get_column_dropper_columns(config, "table1")

        assert result == set()

    def _create_test_config(self):
        """Create a test ClearedConfig."""
        return ClearedConfig(
            name="test_config",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp"},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp"},
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp"},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp"},
                    ),
                ),
                runtime_io_path="/tmp",
            ),
            tables={
                "table1": TableConfig(
                    name="table1",
                    transformers=[],
                ),
            },
        )


class TestLoadDataForTable:
    """Test load_data_for_table function."""

    @patch("cleared.cli.cmds.verify.utils.FileSystemDataLoader")
    def test_successful_load(self, mock_loader_class):
        """Test successful data loading."""
        config = self._create_test_config()
        mock_loader = MagicMock()
        mock_loader.read_table.return_value = pd.DataFrame({"col1": [1, 2]})
        mock_loader_class.return_value = mock_loader

        result = load_data_for_table(config, "table1", Path("/tmp/data"))

        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert "col1" in result.columns
        mock_loader.read_table.assert_called_once_with("table1")

    @patch("cleared.cli.cmds.verify.utils.FileSystemDataLoader")
    def test_load_failure(self, mock_loader_class):
        """Test data loading failure."""
        config = self._create_test_config()
        mock_loader = MagicMock()
        mock_loader.read_table.side_effect = Exception("Load failed")
        mock_loader_class.return_value = mock_loader

        result = load_data_for_table(config, "table1", Path("/tmp/data"))

        assert result is None

    def test_non_filesystem_io(self):
        """Test with non-filesystem IO type."""
        config = self._create_test_config()
        config.io.data.input_config.io_type = "sql"

        result = load_data_for_table(config, "table1", Path("/tmp/data"))

        assert result is None

    @patch("cleared.cli.cmds.verify.utils.FileSystemDataLoader")
    def test_file_format_from_config(self, mock_loader_class):
        """Test that file format is taken from config."""
        config = self._create_test_config()
        config.io.data.input_config.configs["file_format"] = "parquet"
        mock_loader = MagicMock()
        mock_loader.read_table.return_value = pd.DataFrame()
        mock_loader_class.return_value = mock_loader

        load_data_for_table(config, "table1", Path("/tmp/data"))

        # Verify loader was created with correct config
        call_args = mock_loader_class.call_args[0][0]
        assert call_args["connection_params"]["file_format"] == "parquet"

    @patch("cleared.cli.cmds.verify.utils.FileSystemDataLoader")
    def test_default_file_format(self, mock_loader_class):
        """Test default file format when not specified."""
        config = self._create_test_config()
        del config.io.data.input_config.configs["file_format"]
        mock_loader = MagicMock()
        mock_loader.read_table.return_value = pd.DataFrame()
        mock_loader_class.return_value = mock_loader

        load_data_for_table(config, "table1", Path("/tmp/data"))

        # Verify default CSV format
        call_args = mock_loader_class.call_args[0][0]
        assert call_args["connection_params"]["file_format"] == "csv"

    def _create_test_config(self):
        """Create a test ClearedConfig."""
        return ClearedConfig(
            name="test_config",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp", "file_format": "csv"},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp"},
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp"},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp"},
                    ),
                ),
                runtime_io_path="/tmp",
            ),
            tables={},
        )


class TestPrintVerificationResults:
    """Test print_verification_results function."""

    @patch("cleared.cli.cmds.verify.utils.typer.echo")
    def test_print_results(self, mock_echo):
        """Test printing verification results."""
        result = self._create_test_result()

        print_verification_results(result)

        # Verify that typer.echo was called multiple times
        assert mock_echo.call_count > 0
        # Check that overview stats are printed
        call_args = [str(call[0][0]) for call in mock_echo.call_args_list]
        overview_printed = any("Total Tables" in arg for arg in call_args)
        assert overview_printed

    @patch("cleared.cli.cmds.verify.utils.typer.echo")
    def test_print_with_errors(self, mock_echo):
        """Test printing results with errors."""
        result = self._create_test_result_with_errors()

        print_verification_results(result)

        # Verify errors are printed
        call_args = [str(call[0][0]) for call in mock_echo.call_args_list]
        errors_printed = any("Errors" in arg for arg in call_args)
        assert errors_printed

    @patch("cleared.cli.cmds.verify.utils.typer.echo")
    def test_print_with_warnings(self, mock_echo):
        """Test printing results with warnings."""
        result = self._create_test_result_with_warnings()

        print_verification_results(result)

        # Verify warnings are printed
        call_args = [str(call[0][0]) for call in mock_echo.call_args_list]
        warnings_printed = any("Warnings" in arg for arg in call_args)
        assert warnings_printed

    @patch("cleared.cli.cmds.verify.utils.typer.echo")
    def test_print_many_errors_truncated(self, mock_echo):
        """Test that many errors are truncated."""
        result = self._create_test_result_with_many_errors()

        print_verification_results(result)

        # Verify truncation message is printed
        call_args = [str(call[0][0]) for call in mock_echo.call_args_list]
        truncation_printed = any("more errors" in arg for arg in call_args)
        assert truncation_printed

    def _create_test_result(self):
        """Create a test VerificationResult."""
        col_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "pass", "Perfect", 10, 10, 0, 0.0),
        ]
        return VerificationResult(
            overview=VerificationOverview(
                total_tables=1,
                passed_tables=1,
                failed_tables=0,
                warning_tables=0,
                total_errors=0,
                total_warnings=0,
                total_columns_checked=2,
                total_columns_passed=2,
                total_columns_errored=0,
                total_columns_warned=0,
            ),
            tables=[
                TableVerificationResult(
                    "table1",
                    "pass",
                    2,
                    2,
                    0,
                    0,
                    [],
                    [],
                    col_results,
                ),
            ],
            config_path="test_config",
            reverse_data_path="/tmp/reversed",
        )

    def _create_test_result_with_errors(self):
        """Create a test VerificationResult with errors."""
        col_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "error", "Test error", 10, 10, 5, 50.0),
        ]
        return VerificationResult(
            overview=VerificationOverview(
                total_tables=1,
                passed_tables=0,
                failed_tables=1,
                warning_tables=0,
                total_errors=1,
                total_warnings=0,
                total_columns_checked=2,
                total_columns_passed=1,
                total_columns_errored=1,
                total_columns_warned=0,
            ),
            tables=[
                TableVerificationResult(
                    "table1",
                    "error",
                    2,
                    1,
                    1,
                    0,
                    ["Test error"],
                    [],
                    col_results,
                ),
            ],
            config_path="test_config",
            reverse_data_path="/tmp/reversed",
        )

    def _create_test_result_with_warnings(self):
        """Create a test VerificationResult with warnings."""
        col_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "warning", "Test warning", 10, 0, 0, 0.0),
        ]
        return VerificationResult(
            overview=VerificationOverview(
                total_tables=1,
                passed_tables=0,
                failed_tables=0,
                warning_tables=1,
                total_errors=0,
                total_warnings=1,
                total_columns_checked=2,
                total_columns_passed=1,
                total_columns_errored=0,
                total_columns_warned=1,
            ),
            tables=[
                TableVerificationResult(
                    "table1",
                    "warning",
                    2,
                    1,
                    0,
                    1,
                    [],
                    ["Test warning"],
                    col_results,
                ),
            ],
            config_path="test_config",
            reverse_data_path="/tmp/reversed",
        )

    def _create_test_result_with_many_errors(self):
        """Create a test VerificationResult with many errors."""
        col_results = [
            ColumnComparisonResult(f"col{i}", "error", f"Error {i}", 10, 10, 5, 50.0)
            for i in range(10)
        ]
        return VerificationResult(
            overview=VerificationOverview(
                total_tables=1,
                passed_tables=0,
                failed_tables=1,
                warning_tables=0,
                total_errors=10,
                total_warnings=0,
                total_columns_checked=10,
                total_columns_passed=0,
                total_columns_errored=10,
                total_columns_warned=0,
            ),
            tables=[
                TableVerificationResult(
                    "table1",
                    "error",
                    10,
                    0,
                    10,
                    0,
                    [f"Error {i}" for i in range(10)],
                    [],
                    col_results,
                ),
            ],
            config_path="test_config",
            reverse_data_path="/tmp/reversed",
        )
