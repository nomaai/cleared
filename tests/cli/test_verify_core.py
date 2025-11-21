"""Unit tests for verify core functions."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from cleared.cli.cmds.verify.core import (
    _compare_column_values,
    _create_error_status_result,
    _handle_extra_columns,
    _handle_length_mismatch,
    _handle_missing_reversed_column,
    _prepare_table_verification_result,
    _prepare_verification_result,
    compare_column,
    verify_data,
    verify_table,
)
from cleared.cli.cmds.verify.model import (
    ColumnComparisonResult,
    TableVerificationResult,
)
from cleared.config.structure import (
    ClearedConfig,
    ClearedIOConfig,
    DeIDConfig,
    IOConfig,
    PairedIOConfig,
)


class TestCompareColumn:
    """Test compare_column function."""

    def test_perfect_match(self):
        """Test comparing identical columns."""
        original = pd.Series([1, 2, 3])
        reversed_series = pd.Series([1, 2, 3])

        result = compare_column(original, reversed_series, "test_col", False)

        assert result.status == "pass"
        assert result.column_name == "test_col"
        assert result.mismatch_count == 0
        assert result.mismatch_percentage == 0.0
        assert result.original_length == 3
        assert result.reversed_length == 3

    def test_mismatches(self):
        """Test comparing columns with mismatches."""
        original = pd.Series([1, 2, 3])
        reversed_series = pd.Series([1, 5, 3])

        result = compare_column(original, reversed_series, "test_col", False)

        assert result.status == "error"
        assert result.mismatch_count == 1
        assert result.mismatch_percentage == pytest.approx(33.33, abs=0.01)
        assert len(result.sample_mismatch_indices) == 1

    def test_missing_reversed_column_not_dropped(self):
        """Test missing column when not dropped."""
        original = pd.Series([1, 2, 3])

        result = compare_column(original, None, "test_col", False)

        assert result.status == "error"
        assert "missing in reversed data" in result.message
        assert result.original_length == 3
        assert result.reversed_length == 0

    def test_missing_reversed_column_dropped(self):
        """Test missing column when dropped."""
        original = pd.Series([1, 2, 3])

        result = compare_column(original, None, "test_col", True)

        assert result.status == "warning"
        assert "dropped by ColumnDropper" in result.message
        assert result.original_length == 3
        assert result.reversed_length == 0

    def test_length_mismatch(self):
        """Test length mismatch."""
        original = pd.Series([1, 2, 3])
        reversed_series = pd.Series([1, 2])

        result = compare_column(original, reversed_series, "test_col", False)

        assert result.status == "error"
        assert "length mismatch" in result.message
        assert result.original_length == 3
        assert result.reversed_length == 2

    def test_nan_handling(self):
        """Test NaN value handling."""
        original = pd.Series([1.0, float("nan"), 3.0])
        reversed_series = pd.Series([1.0, float("nan"), 3.0])

        result = compare_column(original, reversed_series, "test_col", False)

        assert result.status == "pass"
        assert result.mismatch_count == 0

    def test_nan_mismatch(self):
        """Test NaN vs non-NaN mismatch."""
        original = pd.Series([1.0, float("nan"), 3.0])
        reversed_series = pd.Series([1.0, 2.0, 3.0])

        result = compare_column(original, reversed_series, "test_col", False)

        assert result.status == "error"
        assert result.mismatch_count == 1

    def test_many_mismatches_sample_limit(self):
        """Test that sample mismatch indices are limited to 100."""
        original = pd.Series(range(200))
        reversed_series = pd.Series([x if x < 100 else x + 1 for x in range(200)])

        result = compare_column(original, reversed_series, "test_col", False)

        assert result.status == "error"
        assert result.mismatch_count == 100
        assert len(result.sample_mismatch_indices) == 100


class TestHandleMissingReversedColumn:
    """Test _handle_missing_reversed_column function."""

    def test_dropped_column(self):
        """Test dropped column case."""
        result = _handle_missing_reversed_column("test_col", 10, True)

        assert result.status == "warning"
        assert "dropped by ColumnDropper" in result.message
        assert result.original_length == 10
        assert result.reversed_length == 0

    def test_missing_column(self):
        """Test missing column case."""
        result = _handle_missing_reversed_column("test_col", 10, False)

        assert result.status == "error"
        assert "missing in reversed data" in result.message
        assert result.original_length == 10
        assert result.reversed_length == 0


class TestHandleLengthMismatch:
    """Test _handle_length_mismatch function."""

    def test_length_mismatch(self):
        """Test length mismatch handling."""
        result = _handle_length_mismatch("test_col", 10, 5)

        assert result.status == "error"
        assert "length mismatch" in result.message
        assert result.original_length == 10
        assert result.reversed_length == 5


class TestCompareColumnValues:
    """Test _compare_column_values function."""

    def test_perfect_match(self):
        """Test perfect match."""
        original = pd.Series([1, 2, 3])
        reversed_series = pd.Series([1, 2, 3])

        result = _compare_column_values("test_col", original, reversed_series, 3, 3)

        assert result.status == "pass"
        assert result.mismatch_count == 0
        assert result.mismatch_percentage == 0.0

    def test_mismatches(self):
        """Test value mismatches."""
        original = pd.Series([1, 2, 3, 4])
        reversed_series = pd.Series([1, 5, 3, 6])

        result = _compare_column_values("test_col", original, reversed_series, 4, 4)

        assert result.status == "error"
        assert result.mismatch_count == 2
        assert result.mismatch_percentage == 50.0
        assert len(result.sample_mismatch_indices) == 2

    def test_nan_handling(self):
        """Test NaN handling."""
        original = pd.Series([1.0, float("nan"), 3.0])
        reversed_series = pd.Series([1.0, float("nan"), 3.0])

        result = _compare_column_values("test_col", original, reversed_series, 3, 3)

        assert result.status == "pass"
        assert result.mismatch_count == 0


class TestHandleExtraColumns:
    """Test _handle_extra_columns function."""

    def test_no_extra_columns(self):
        """Test when no extra columns exist."""
        reversed_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        reversed_columns = {"col1", "col2"}
        original_columns = {"col1", "col2"}
        errors = []
        column_results = []

        _handle_extra_columns(
            reversed_df, reversed_columns, original_columns, errors, column_results
        )

        assert len(errors) == 0
        assert len(column_results) == 0

    def test_extra_columns(self):
        """Test when extra columns exist."""
        reversed_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4], "extra": [5, 6]})
        reversed_columns = {"col1", "col2", "extra"}
        original_columns = {"col1", "col2"}
        errors = []
        column_results = []

        _handle_extra_columns(
            reversed_df, reversed_columns, original_columns, errors, column_results
        )

        assert len(errors) == 1
        assert "extra" in errors[0]
        assert len(column_results) == 1
        assert column_results[0].column_name == "extra"
        assert column_results[0].status == "error"

    def test_multiple_extra_columns(self):
        """Test multiple extra columns."""
        reversed_df = pd.DataFrame(
            {
                "col1": [1, 2],
                "extra1": [3, 4],
                "extra2": [5, 6],
            }
        )
        reversed_columns = {"col1", "extra1", "extra2"}
        original_columns = {"col1"}
        errors = []
        column_results = []

        _handle_extra_columns(
            reversed_df, reversed_columns, original_columns, errors, column_results
        )

        assert len(errors) == 2
        assert len(column_results) == 2
        assert {r.column_name for r in column_results} == {"extra1", "extra2"}


class TestPrepareTableVerificationResult:
    """Test _prepare_table_verification_result function."""

    def test_all_pass(self):
        """Test all columns pass."""
        column_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "pass", "Perfect", 10, 10, 0, 0.0),
        ]

        result = _prepare_table_verification_result("table1", column_results, [], [])

        assert result.table_name == "table1"
        assert result.status == "pass"
        assert result.total_columns == 2
        assert result.passed_columns == 2
        assert result.error_columns == 0
        assert result.warning_columns == 0

    def test_with_errors(self):
        """Test with error columns."""
        column_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "error", "Mismatch", 10, 10, 5, 50.0),
        ]
        errors = ["Mismatch"]

        result = _prepare_table_verification_result(
            "table1", column_results, errors, []
        )

        assert result.status == "error"
        assert result.passed_columns == 1
        assert result.error_columns == 1

    def test_with_warnings(self):
        """Test with warning columns."""
        column_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "warning", "Dropped", 10, 0, 0, 0.0),
        ]
        warnings = ["Dropped"]

        result = _prepare_table_verification_result(
            "table1", column_results, [], warnings
        )

        assert result.status == "warning"
        assert result.passed_columns == 1
        assert result.warning_columns == 1

    def test_mixed_status(self):
        """Test mixed status (error takes precedence)."""
        column_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "error", "Mismatch", 10, 10, 5, 50.0),
            ColumnComparisonResult("col3", "warning", "Dropped", 10, 0, 0, 0.0),
        ]
        errors = ["Mismatch"]
        warnings = ["Dropped"]

        result = _prepare_table_verification_result(
            "table1", column_results, errors, warnings
        )

        assert result.status == "error"
        assert result.passed_columns == 1
        assert result.error_columns == 1
        assert result.warning_columns == 1


class TestCreateErrorStatusResult:
    """Test _create_error_status_result function."""

    def test_error_result_creation(self):
        """Test creating error status result."""
        result = _create_error_status_result("table1", "Test error message")

        assert result.table_name == "table1"
        assert result.status == "error"
        assert result.total_columns == 1
        assert result.error_columns == 1
        assert len(result.errors) == 1
        assert result.errors[0] == "Test error message"
        assert len(result.column_results) == 1
        assert result.column_results[0].status == "error"


class TestVerifyTable:
    """Test verify_table function."""

    def test_perfect_match(self):
        """Test perfect table match."""
        config = MagicMock()
        original_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        reversed_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        result = verify_table(config, "table1", original_df, reversed_df, set())

        assert result.table_name == "table1"
        assert result.status == "pass"
        assert result.total_columns == 2
        assert result.passed_columns == 2

    def test_with_mismatches(self):
        """Test table with mismatches."""
        config = MagicMock()
        original_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        reversed_df = pd.DataFrame({"col1": [1, 5], "col2": [3, 4]})

        result = verify_table(config, "table1", original_df, reversed_df, set())

        assert result.status == "error"
        assert result.error_columns == 1

    def test_with_dropped_columns(self):
        """Test table with dropped columns."""
        config = MagicMock()
        original_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        reversed_df = pd.DataFrame({"col1": [1, 2]})

        result = verify_table(config, "table1", original_df, reversed_df, {"col2"})

        assert result.status == "warning"
        assert result.warning_columns == 1

    def test_with_extra_columns(self):
        """Test table with extra columns in reversed."""
        config = MagicMock()
        original_df = pd.DataFrame({"col1": [1, 2]})
        reversed_df = pd.DataFrame({"col1": [1, 2], "extra": [3, 4]})

        result = verify_table(config, "table1", original_df, reversed_df, set())

        assert result.status == "error"
        assert result.error_columns == 1
        assert any("extra" in err for err in result.errors)

    def test_empty_reversed_df(self):
        """Test with empty reversed DataFrame."""
        config = MagicMock()
        original_df = pd.DataFrame({"col1": [1, 2]})
        reversed_df = pd.DataFrame()

        result = verify_table(config, "table1", original_df, reversed_df, set())

        assert result.status == "error"
        assert result.error_columns == 1

    def test_none_reversed_df(self):
        """Test with None reversed DataFrame."""
        config = MagicMock()
        original_df = pd.DataFrame({"col1": [1, 2]})

        result = verify_table(config, "table1", original_df, None, set())

        assert result.status == "error"
        assert result.error_columns == 1


class TestPrepareVerificationResult:
    """Test _prepare_verification_result function."""

    def test_single_table_pass(self):
        """Test single table passing."""
        col_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "pass", "Perfect", 10, 10, 0, 0.0),
        ]
        table_results = [
            TableVerificationResult("table1", "pass", 2, 2, 0, 0, [], [], col_results)
        ]

        result = _prepare_verification_result(
            table_results, "config1", Path("/tmp/reversed")
        )

        assert result.config_path == "config1"
        assert result.reverse_data_path == "/tmp/reversed"
        assert result.overview.total_tables == 1
        assert result.overview.passed_tables == 1
        assert result.overview.failed_tables == 0

    def test_multiple_tables_mixed(self):
        """Test multiple tables with mixed status."""
        col_results1 = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "pass", "Perfect", 10, 10, 0, 0.0),
        ]
        col_results2 = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "error", "Error", 10, 10, 5, 50.0),
        ]
        col_results3 = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "warning", "Warning", 10, 0, 0, 0.0),
        ]
        table_results = [
            TableVerificationResult("table1", "pass", 2, 2, 0, 0, [], [], col_results1),
            TableVerificationResult(
                "table2", "error", 2, 1, 1, 0, ["Error"], [], col_results2
            ),
            TableVerificationResult(
                "table3", "warning", 2, 1, 0, 1, [], ["Warning"], col_results3
            ),
        ]

        result = _prepare_verification_result(
            table_results, "config1", Path("/tmp/reversed")
        )

        assert result.overview.total_tables == 3
        assert result.overview.passed_tables == 1
        assert result.overview.failed_tables == 1
        assert result.overview.warning_tables == 1
        assert result.overview.total_errors == 1
        assert result.overview.total_warnings == 1
        assert result.overview.total_columns_checked == 6
        assert result.overview.total_columns_passed == 4

    def test_empty_tables(self):
        """Test with no tables."""
        table_results = []

        result = _prepare_verification_result(
            table_results, "config1", Path("/tmp/reversed")
        )

        assert result.overview.total_tables == 0
        assert result.overview.passed_tables == 0


class TestVerifyData:
    """Test verify_data function."""

    def test_single_table_success(self):
        """Test successful verification of single table."""
        config = self._create_test_config()

        def load_data_fn(cfg, table_name, data_path):
            if table_name == "table1":
                return pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
            return None

        def get_dropped_columns_fn(cfg, table_name):
            return set()

        result = verify_data(
            config,
            Path("/tmp/reversed"),
            load_data_fn,
            get_dropped_columns_fn,
        )

        assert result.overview.total_tables == 1
        assert len(result.tables) == 1

    def test_multiple_tables(self):
        """Test verification of multiple tables."""
        config = self._create_test_config()
        config.tables = {
            "table1": MagicMock(),
            "table2": MagicMock(),
        }

        def load_data_fn(cfg, table_name, data_path):
            return pd.DataFrame({"col1": [1, 2]})

        def get_dropped_columns_fn(cfg, table_name):
            return set()

        result = verify_data(
            config,
            Path("/tmp/reversed"),
            load_data_fn,
            get_dropped_columns_fn,
        )

        assert result.overview.total_tables == 2

    def test_original_data_load_failure(self):
        """Test when original data fails to load."""
        config = self._create_test_config()

        def load_data_fn(cfg, table_name, data_path):
            return None

        def get_dropped_columns_fn(cfg, table_name):
            return set()

        result = verify_data(
            config,
            Path("/tmp/reversed"),
            load_data_fn,
            get_dropped_columns_fn,
        )

        assert result.overview.total_tables == 1
        assert result.overview.failed_tables == 1
        # The error message will be from the engine's exception handling
        assert len(result.tables[0].errors) > 0
        # Check that it's a pipeline verification error
        assert (
            "verification failed" in result.tables[0].errors[0]
            or "table" in result.tables[0].errors[0].lower()
        )

    def test_reversed_data_load_failure(self):
        """Test when reversed data fails to load."""
        config = self._create_test_config()

        def load_data_fn(cfg, table_name, data_path):
            if str(data_path) == "/tmp/original":
                return pd.DataFrame({"col1": [1, 2]})
            return None

        def get_dropped_columns_fn(cfg, table_name):
            return set()

        result = verify_data(
            config,
            Path("/tmp/reversed"),
            load_data_fn,
            get_dropped_columns_fn,
        )

        assert result.overview.failed_tables == 1
        # The error message will be from the engine's exception handling
        assert len(result.tables[0].errors) > 0
        # Check that it's a pipeline verification error
        assert (
            "verification failed" in result.tables[0].errors[0]
            or "table" in result.tables[0].errors[0].lower()
        )

    def _create_test_config(self):
        """Create a test ClearedConfig."""
        config = ClearedConfig(
            name="test_config",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp/original"},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp/output"},
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp/deid_ref"},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp/deid_ref"},
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={"table1": MagicMock()},
        )
        return config
