"""Unit tests for verify model validation functions."""

from __future__ import annotations

import pytest

from cleared.cli.cmds.verify.model import (
    ColumnComparisonResult,
    TableVerificationResult,
    VerificationOverview,
    VerificationResult,
)


class TestColumnComparisonResultValidation:
    """Test ColumnComparisonResult validation."""

    def test_valid_pass_result(self):
        """Test valid pass result."""
        result = ColumnComparisonResult("col1", "pass", "Perfect match", 10, 10, 0, 0.0)
        assert result.status == "pass"
        assert result.mismatch_count == 0

    def test_valid_error_result(self):
        """Test valid error result."""
        result = ColumnComparisonResult(
            "col1", "error", "Mismatch", 10, 10, 5, 50.0, [0, 1, 2]
        )
        assert result.status == "error"
        assert result.mismatch_count == 5

    def test_invalid_status(self):
        """Test invalid status raises error."""
        with pytest.raises(ValueError, match="Invalid status"):
            ColumnComparisonResult("col1", "invalid", "test")

    def test_empty_column_name(self):
        """Test empty column name raises error."""
        with pytest.raises(ValueError, match="column_name must be a non-empty string"):
            ColumnComparisonResult("", "pass", "test")

    def test_empty_message(self):
        """Test empty message raises error."""
        with pytest.raises(ValueError, match="message must be a non-empty string"):
            ColumnComparisonResult("col1", "pass", "")

    def test_pass_with_mismatches(self):
        """Test pass status with mismatches raises error."""
        with pytest.raises(ValueError, match="Pass status requires mismatch_count"):
            ColumnComparisonResult("col1", "pass", "test", mismatch_count=5)

    def test_pass_with_mismatch_percentage(self):
        """Test pass status with mismatch percentage raises error."""
        with pytest.raises(
            ValueError, match="Pass status requires mismatch_percentage"
        ):
            ColumnComparisonResult("col1", "pass", "test", mismatch_percentage=50.0)

    def test_pass_with_mismatch_indices(self):
        """Test pass status with mismatch indices raises error."""
        with pytest.raises(
            ValueError, match="Pass status should have no mismatch indices"
        ):
            ColumnComparisonResult(
                "col1", "pass", "test", sample_mismatch_indices=[0, 1]
            )

    def test_mismatch_count_exceeds_length(self):
        """Test mismatch count exceeding original length raises error."""
        with pytest.raises(ValueError, match="cannot exceed original_length"):
            ColumnComparisonResult(
                "col1", "error", "test", original_length=10, mismatch_count=15
            )

    def test_invalid_mismatch_percentage(self):
        """Test invalid mismatch percentage raises error."""
        with pytest.raises(ValueError, match=r"must be between 0\.0 and 100\.0"):
            ColumnComparisonResult("col1", "error", "test", mismatch_percentage=150.0)

    def test_mismatch_percentage_inconsistency(self):
        """Test mismatch percentage inconsistency raises error."""
        with pytest.raises(ValueError, match="does not match calculated value"):
            ColumnComparisonResult(
                "col1",
                "error",
                "test",
                original_length=10,
                mismatch_count=5,
                mismatch_percentage=60.0,  # Should be 50.0
            )

    def test_sample_indices_without_mismatches(self):
        """Test sample indices without mismatches raises error."""
        with pytest.raises(
            ValueError, match="should be empty when mismatch_count is 0"
        ):
            ColumnComparisonResult(
                "col1", "error", "test", sample_mismatch_indices=[0, 1]
            )

    def test_sample_indices_exceed_mismatch_count(self):
        """Test sample indices exceeding mismatch count raises error."""
        with pytest.raises(ValueError, match="cannot exceed mismatch_count"):
            ColumnComparisonResult(
                "col1",
                "error",
                "test",
                original_length=10,
                mismatch_count=2,
                mismatch_percentage=20.0,
                sample_mismatch_indices=[0, 1, 2, 3],
            )

    def test_sample_indices_exceed_original_length(self):
        """Test sample indices exceeding original length raises error."""
        with pytest.raises(ValueError, match="exceed original_length"):
            ColumnComparisonResult(
                "col1",
                "error",
                "test",
                original_length=5,
                mismatch_count=3,
                mismatch_percentage=60.0,
                sample_mismatch_indices=[0, 1, 10],  # 10 exceeds length
            )


class TestTableVerificationResultValidation:
    """Test TableVerificationResult validation."""

    def test_valid_result(self):
        """Test valid table result."""
        col_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "pass", "Perfect", 10, 10, 0, 0.0),
        ]
        result = TableVerificationResult(
            "table1", "pass", 2, 2, 0, 0, [], [], col_results
        )
        assert result.status == "pass"

    def test_invalid_status(self):
        """Test invalid status raises error."""
        with pytest.raises(ValueError, match="Invalid status"):
            TableVerificationResult("table1", "invalid", 0, 0, 0, 0, [], [], [])

    def test_empty_table_name(self):
        """Test empty table name raises error."""
        with pytest.raises(ValueError, match="table_name must be a non-empty string"):
            TableVerificationResult("", "pass", 0, 0, 0, 0, [], [], [])

    def test_column_counts_dont_add_up(self):
        """Test column counts that don't add up raises error."""
        # Create 5 column results but counts that add up to 4
        col_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col3", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col4", "error", "Error", 10, 10, 5, 50.0),
            ColumnComparisonResult("col5", "warning", "Warning", 10, 0, 0, 0.0),
        ]
        with pytest.raises(ValueError, match="Column counts do not add up"):
            # total_columns=5 but passed(2) + error(1) + warning(1) = 4, not 5
            TableVerificationResult(
                "table1", "error", 5, 2, 1, 1, ["Error"], ["Warning"], col_results
            )

    def test_pass_status_with_errors(self):
        """Test pass status with errors raises error."""
        col_results = [
            ColumnComparisonResult("col1", "error", "Error", 10, 10, 5, 50.0),
        ]
        with pytest.raises(ValueError, match="Pass status requires no errors"):
            TableVerificationResult(
                "table1", "pass", 1, 0, 1, 0, ["Error"], [], col_results
            )

    def test_error_status_without_errors(self):
        """Test error status without errors raises error."""
        col_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
        ]
        with pytest.raises(
            ValueError, match="Error status requires at least one error"
        ):
            TableVerificationResult("table1", "error", 1, 1, 0, 0, [], [], col_results)

    def test_column_results_length_mismatch(self):
        """Test column results length mismatch raises error."""
        col_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
        ]
        with pytest.raises(ValueError, match="Column counts do not add up"):
            TableVerificationResult("table1", "pass", 2, 1, 0, 0, [], [], col_results)

    def test_column_results_counts_mismatch(self):
        """Test column results counts mismatch raises error."""
        col_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "error", "Error", 10, 10, 5, 50.0),
        ]
        with pytest.raises(ValueError, match=r"passed_columns.*does not match"):
            TableVerificationResult(
                "table1", "error", 2, 0, 2, 0, ["Error"], [], col_results
            )

    def test_errors_list_mismatch(self):
        """Test errors list length mismatch raises error."""
        col_results = [
            ColumnComparisonResult("col1", "error", "Error1", 10, 10, 5, 50.0),
            ColumnComparisonResult("col2", "error", "Error2", 10, 10, 5, 50.0),
        ]
        with pytest.raises(ValueError, match="errors list length"):
            TableVerificationResult(
                "table1", "error", 2, 0, 2, 0, ["Error1"], [], col_results
            )


class TestVerificationOverviewValidation:
    """Test VerificationOverview validation."""

    def test_valid_overview(self):
        """Test valid overview."""
        overview = VerificationOverview(
            total_tables=3,
            passed_tables=1,
            failed_tables=1,
            warning_tables=1,
            total_errors=5,
            total_warnings=2,
            total_columns_checked=10,
            total_columns_passed=5,
            total_columns_errored=3,
            total_columns_warned=2,
        )
        assert overview.total_tables == 3

    def test_table_counts_dont_add_up(self):
        """Test table counts that don't add up raises error."""
        with pytest.raises(ValueError, match="Table counts do not add up"):
            VerificationOverview(
                total_tables=5,
                passed_tables=2,
                failed_tables=2,
                warning_tables=2,  # Should be 1
                total_errors=0,
                total_warnings=0,
                total_columns_checked=0,
                total_columns_passed=0,
                total_columns_errored=0,
                total_columns_warned=0,
            )

    def test_column_counts_dont_add_up(self):
        """Test column counts that don't add up raises error."""
        with pytest.raises(ValueError, match="Column counts do not add up"):
            VerificationOverview(
                total_tables=1,
                passed_tables=1,
                failed_tables=0,
                warning_tables=0,
                total_errors=0,
                total_warnings=0,
                total_columns_checked=10,
                total_columns_passed=5,
                total_columns_errored=3,
                total_columns_warned=3,  # Should be 2
            )

    def test_passed_tables_exceed_total(self):
        """Test passed tables exceeding total raises error."""
        with pytest.raises(ValueError, match="Table counts do not add up"):
            VerificationOverview(
                total_tables=5,
                passed_tables=6,  # Exceeds total
                failed_tables=0,
                warning_tables=0,
                total_errors=0,
                total_warnings=0,
                total_columns_checked=0,
                total_columns_passed=0,
                total_columns_errored=0,
                total_columns_warned=0,
            )

    def test_columns_passed_exceed_total(self):
        """Test columns passed exceeding total raises error."""
        with pytest.raises(ValueError, match="Column counts do not add up"):
            VerificationOverview(
                total_tables=1,
                passed_tables=1,
                failed_tables=0,
                warning_tables=0,
                total_errors=0,
                total_warnings=0,
                total_columns_checked=10,
                total_columns_passed=15,  # Exceeds total
                total_columns_errored=0,
                total_columns_warned=0,
            )


class TestVerificationResultValidation:
    """Test VerificationResult validation."""

    def test_valid_result(self):
        """Test valid verification result."""
        col_results = [
            ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
            ColumnComparisonResult("col2", "pass", "Perfect", 10, 10, 0, 0.0),
        ]
        overview = VerificationOverview(
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
        )
        tables = [
            TableVerificationResult("table1", "pass", 2, 2, 0, 0, [], [], col_results),
        ]
        result = VerificationResult(overview, tables, "config.yaml", "/tmp/reversed")
        assert result.config_path == "config.yaml"

    def test_empty_config_path(self):
        """Test empty config path raises error."""
        overview = VerificationOverview(
            total_tables=0,
            passed_tables=0,
            failed_tables=0,
            warning_tables=0,
            total_errors=0,
            total_warnings=0,
            total_columns_checked=0,
            total_columns_passed=0,
            total_columns_errored=0,
            total_columns_warned=0,
        )
        with pytest.raises(ValueError, match="config_path must be a non-empty string"):
            VerificationResult(overview, [], "", "/tmp/reversed")

    def test_empty_reverse_data_path(self):
        """Test empty reverse data path raises error."""
        overview = VerificationOverview(
            total_tables=0,
            passed_tables=0,
            failed_tables=0,
            warning_tables=0,
            total_errors=0,
            total_warnings=0,
            total_columns_checked=0,
            total_columns_passed=0,
            total_columns_errored=0,
            total_columns_warned=0,
        )
        with pytest.raises(
            ValueError, match="reverse_data_path must be a non-empty string"
        ):
            VerificationResult(overview, [], "config.yaml", "")

    def test_tables_length_mismatch(self):
        """Test tables length mismatch raises error."""
        overview = VerificationOverview(
            total_tables=2,
            passed_tables=1,
            failed_tables=1,
            warning_tables=0,
            total_errors=0,
            total_warnings=0,
            total_columns_checked=0,
            total_columns_passed=0,
            total_columns_errored=0,
            total_columns_warned=0,
        )
        tables = [
            TableVerificationResult("table1", "pass", 0, 0, 0, 0, [], [], []),
        ]
        with pytest.raises(ValueError, match=r"tables length.*does not match"):
            VerificationResult(overview, tables, "config.yaml", "/tmp/reversed")

    def test_table_status_counts_mismatch(self):
        """Test table status counts mismatch raises error."""
        overview = VerificationOverview(
            total_tables=1,
            passed_tables=0,  # Should be 1
            failed_tables=1,
            warning_tables=0,
            total_errors=0,
            total_warnings=0,
            total_columns_checked=0,
            total_columns_passed=0,
            total_columns_errored=0,
            total_columns_warned=0,
        )
        tables = [
            TableVerificationResult("table1", "pass", 0, 0, 0, 0, [], [], []),
        ]
        with pytest.raises(
            ValueError, match=r"overview\.passed_tables.*does not match"
        ):
            VerificationResult(overview, tables, "config.yaml", "/tmp/reversed")

    def test_error_counts_mismatch(self):
        """Test error counts mismatch raises error."""
        error_col_result = ColumnComparisonResult(
            "__table_load_error__", "error", "Error", 0, 0, 0, 0.0
        )
        overview = VerificationOverview(
            total_tables=1,
            passed_tables=0,
            failed_tables=1,
            warning_tables=0,
            total_errors=0,  # Should be 1
            total_warnings=0,
            total_columns_checked=1,
            total_columns_passed=0,
            total_columns_errored=1,
            total_columns_warned=0,
        )
        tables = [
            TableVerificationResult(
                "table1", "error", 1, 0, 1, 0, ["Error"], [], [error_col_result]
            ),
        ]
        with pytest.raises(ValueError, match=r"overview\.total_errors.*does not match"):
            VerificationResult(overview, tables, "config.yaml", "/tmp/reversed")

    def test_column_counts_mismatch(self):
        """Test column counts mismatch raises error."""
        # Create overview with mismatched column counts (will fail at overview validation)
        with pytest.raises(ValueError, match="Column counts do not add up"):
            VerificationOverview(
                total_tables=1,
                passed_tables=1,
                failed_tables=0,
                warning_tables=0,
                total_errors=0,
                total_warnings=0,
                total_columns_checked=5,  # Should be 2
                total_columns_passed=2,
                total_columns_errored=0,
                total_columns_warned=0,
            )

    def test_duplicate_table_names(self):
        """Test duplicate table names raises error."""
        overview = VerificationOverview(
            total_tables=2,
            passed_tables=2,
            failed_tables=0,
            warning_tables=0,
            total_errors=0,
            total_warnings=0,
            total_columns_checked=0,
            total_columns_passed=0,
            total_columns_errored=0,
            total_columns_warned=0,
        )
        tables = [
            TableVerificationResult("table1", "pass", 0, 0, 0, 0, [], [], []),
            TableVerificationResult(
                "table1", "pass", 0, 0, 0, 0, [], [], []
            ),  # Duplicate
        ]
        with pytest.raises(ValueError, match="Duplicate table names found"):
            VerificationResult(overview, tables, "config.yaml", "/tmp/reversed")
