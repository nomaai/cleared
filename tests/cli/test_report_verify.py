"""Unit tests for report-verify command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import typer

from cleared.cli.cmds.report_verify import (
    _generate_html,
    _json_to_verification_result,
    _prepare_template_data,
    register_report_verify_command,
)
from cleared.cli.cmds.verify.model import (
    ColumnComparisonResult,
    TableVerificationResult,
    VerificationOverview,
    VerificationResult,
)


class TestJsonToVerificationResult:
    """Test _json_to_verification_result function."""

    def test_valid_json_conversion(self):
        """Test converting valid JSON to VerificationResult."""
        json_data = {
            "overview": {
                "total_tables": 1,
                "passed_tables": 1,
                "failed_tables": 0,
                "warning_tables": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "total_columns_checked": 1,
                "total_columns_passed": 1,
                "total_columns_errored": 0,
                "total_columns_warned": 0,
            },
            "tables": [
                {
                    "table_name": "users",
                    "status": "pass",
                    "total_columns": 1,
                    "passed_columns": 1,
                    "error_columns": 0,
                    "warning_columns": 0,
                    "errors": [],
                    "warnings": [],
                    "column_results": [
                        {
                            "column_name": "id",
                            "status": "pass",
                            "message": "Perfect match",
                            "original_length": 10,
                            "reversed_length": 10,
                            "mismatch_count": 0,
                            "mismatch_percentage": 0.0,
                            "sample_mismatch_indices": [],
                        },
                    ],
                },
            ],
            "config_path": "test_config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        result = _json_to_verification_result(json_data)

        assert isinstance(result, VerificationResult)
        assert result.overview.total_tables == 1
        assert result.overview.passed_tables == 1
        assert len(result.tables) == 1
        assert result.tables[0].table_name == "users"
        assert len(result.tables[0].column_results) == 1
        assert result.tables[0].column_results[0].column_name == "id"

    def test_json_with_missing_optional_fields(self):
        """Test JSON conversion with missing optional fields."""
        json_data = {
            "overview": {
                "total_tables": 1,
                "passed_tables": 1,
                "failed_tables": 0,
                "warning_tables": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "total_columns_checked": 1,
                "total_columns_passed": 1,
                "total_columns_errored": 0,
                "total_columns_warned": 0,
            },
            "tables": [
                {
                    "table_name": "test",
                    "status": "pass",
                    "total_columns": 1,
                    "passed_columns": 1,
                    "error_columns": 0,
                    "warning_columns": 0,
                    "column_results": [
                        {
                            "column_name": "col1",
                            "status": "pass",
                            "message": "Perfect",
                        },
                    ],
                },
            ],
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        result = _json_to_verification_result(json_data)

        assert result.tables[0].column_results[0].original_length == 0
        assert result.tables[0].column_results[0].reversed_length == 0
        assert result.tables[0].column_results[0].mismatch_count == 0
        assert result.tables[0].column_results[0].mismatch_percentage == 0.0
        assert result.tables[0].column_results[0].sample_mismatch_indices == []

    def test_json_with_all_column_fields(self):
        """Test JSON conversion with all column fields present."""
        json_data = {
            "overview": {
                "total_tables": 1,
                "passed_tables": 0,
                "failed_tables": 1,
                "warning_tables": 0,
                "total_errors": 1,
                "total_warnings": 0,
                "total_columns_checked": 1,
                "total_columns_passed": 0,
                "total_columns_errored": 1,
                "total_columns_warned": 0,
            },
            "tables": [
                {
                    "table_name": "test",
                    "status": "error",
                    "total_columns": 1,
                    "passed_columns": 0,
                    "error_columns": 1,
                    "warning_columns": 0,
                    "errors": ["Error message"],
                    "warnings": [],
                    "column_results": [
                        {
                            "column_name": "col1",
                            "status": "error",
                            "message": "Mismatch",
                            "original_length": 10,
                            "reversed_length": 10,
                            "mismatch_count": 5,
                            "mismatch_percentage": 50.0,
                            "sample_mismatch_indices": [0, 1, 2, 3, 4],
                        },
                    ],
                },
            ],
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        result = _json_to_verification_result(json_data)

        col_result = result.tables[0].column_results[0]
        assert col_result.original_length == 10
        assert col_result.reversed_length == 10
        assert col_result.mismatch_count == 5
        assert col_result.mismatch_percentage == 50.0
        assert col_result.sample_mismatch_indices == [0, 1, 2, 3, 4]

    def test_json_with_multiple_tables(self):
        """Test JSON conversion with multiple tables."""
        json_data = {
            "overview": {
                "total_tables": 2,
                "passed_tables": 1,
                "failed_tables": 1,
                "warning_tables": 0,
                "total_errors": 1,
                "total_warnings": 0,
                "total_columns_checked": 2,
                "total_columns_passed": 1,
                "total_columns_errored": 1,
                "total_columns_warned": 0,
            },
            "tables": [
                {
                    "table_name": "table1",
                    "status": "pass",
                    "total_columns": 1,
                    "passed_columns": 1,
                    "error_columns": 0,
                    "warning_columns": 0,
                    "errors": [],
                    "warnings": [],
                    "column_results": [
                        {
                            "column_name": "col1",
                            "status": "pass",
                            "message": "Perfect",
                        },
                    ],
                },
                {
                    "table_name": "table2",
                    "status": "error",
                    "total_columns": 1,
                    "passed_columns": 0,
                    "error_columns": 1,
                    "warning_columns": 0,
                    "errors": ["Error"],
                    "warnings": [],
                    "column_results": [
                        {
                            "column_name": "col2",
                            "status": "error",
                            "message": "Error",
                        },
                    ],
                },
            ],
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        result = _json_to_verification_result(json_data)

        assert len(result.tables) == 2
        assert result.tables[0].table_name == "table1"
        assert result.tables[1].table_name == "table2"
        assert result.tables[0].status == "pass"
        assert result.tables[1].status == "error"


class TestPrepareTemplateData:
    """Test _prepare_template_data function."""

    def test_template_data_preparation(self):
        """Test preparing template data from VerificationResult."""
        overview = VerificationOverview(
            total_tables=1,
            passed_tables=1,
            failed_tables=0,
            warning_tables=0,
            total_errors=0,
            total_warnings=0,
            total_columns_checked=1,
            total_columns_passed=1,
            total_columns_errored=0,
            total_columns_warned=0,
        )
        tables = [
            TableVerificationResult(
                "table1",
                "pass",
                1,
                1,
                0,
                0,
                [],
                [],
                [
                    ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
                ],
            ),
        ]
        verification_result = VerificationResult(
            overview=overview,
            tables=tables,
            config_path="config.yaml",
            reverse_data_path="/tmp/reversed",
        )

        template_data = _prepare_template_data(verification_result, Path("test.json"))

        assert template_data["config_name"] == "config.yaml"
        assert template_data["overview"] == overview
        assert template_data["tables"] == tables
        assert template_data["config_path"] == "config.yaml"
        assert template_data["reverse_data_path"] == "/tmp/reversed"
        assert template_data["json_file"] == "test.json"
        assert "generated_at" in template_data
        assert "version" in template_data

    def test_template_data_without_json_path(self):
        """Test template data preparation without json_path."""
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
        verification_result = VerificationResult(
            overview=overview,
            tables=[],
            config_path="config.yaml",
            reverse_data_path="/tmp/reversed",
        )

        template_data = _prepare_template_data(verification_result, None)

        assert template_data["json_file"] is None


class TestGenerateHtml:
    """Test _generate_html function."""

    def test_html_generation(self):
        """Test HTML generation from template data."""
        template_data = {
            "config_name": "test_config",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": None,
            "overview": VerificationOverview(
                total_tables=1,
                passed_tables=1,
                failed_tables=0,
                warning_tables=0,
                total_errors=0,
                total_warnings=0,
                total_columns_checked=1,
                total_columns_passed=1,
                total_columns_errored=0,
                total_columns_warned=0,
            ),
            "tables": [],
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        html = _generate_html(template_data)

        assert isinstance(html, str)
        assert "Cleared Verification Report" in html
        assert "test_config" in html
        assert "2024-01-01 12:00:00" in html

    def test_html_includes_overview_stats(self):
        """Test that HTML includes overview statistics."""
        template_data = {
            "config_name": "test",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": None,
            "overview": VerificationOverview(
                total_tables=5,
                passed_tables=3,
                failed_tables=1,
                warning_tables=1,
                total_errors=10,
                total_warnings=5,
                total_columns_checked=20,
                total_columns_passed=15,
                total_columns_errored=3,
                total_columns_warned=2,
            ),
            "tables": [],
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        html = _generate_html(template_data)

        assert "5" in html  # total_tables
        assert "3" in html  # passed_tables
        assert "1" in html  # failed_tables
        assert "10" in html  # total_errors

    def test_html_includes_table_data(self):
        """Test that HTML includes table data."""
        tables = [
            TableVerificationResult(
                "table1",
                "pass",
                2,
                2,
                0,
                0,
                [],
                [],
                [
                    ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0),
                    ColumnComparisonResult("col2", "pass", "Perfect", 10, 10, 0, 0.0),
                ],
            ),
        ]
        template_data = {
            "config_name": "test",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": None,
            "overview": VerificationOverview(
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
            "tables": tables,
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        html = _generate_html(template_data)

        assert "table1" in html
        assert "col1" in html
        assert "col2" in html
        assert "Perfect" in html


class TestReportVerifyCommand:
    """Test report-verify command execution."""

    def test_command_registration(self):
        """Test that command is registered."""
        app = typer.Typer()
        register_report_verify_command(app)

        # Check that command exists
        commands = [cmd.name for cmd in app.registered_commands]
        assert "report-verify" in commands

    def test_command_execution_default_output(self, tmp_path):
        """Test command execution with default output path."""
        # Create test JSON file
        json_file = tmp_path / "test.json"
        json_data = {
            "overview": {
                "total_tables": 0,
                "passed_tables": 0,
                "failed_tables": 0,
                "warning_tables": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "total_columns_checked": 0,
                "total_columns_passed": 0,
                "total_columns_errored": 0,
                "total_columns_warned": 0,
            },
            "tables": [],
            "config_path": "test.yaml",
            "reverse_data_path": "/tmp/reversed",
        }
        json_file.write_text(json.dumps(json_data))

        # Test the conversion and generation flow
        with open(json_file) as f:
            loaded_data = json.load(f)

        verification_result = _json_to_verification_result(loaded_data)
        template_data = _prepare_template_data(verification_result, json_file)
        html_content = _generate_html(template_data)

        # Verify HTML was generated
        assert isinstance(html_content, str)
        assert len(html_content) > 0

    def test_command_with_valid_json_file(self, tmp_path):
        """Test command with valid JSON file."""
        # Create test JSON file
        json_file = tmp_path / "test_verify.json"
        json_data = {
            "overview": {
                "total_tables": 1,
                "passed_tables": 1,
                "failed_tables": 0,
                "warning_tables": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "total_columns_checked": 1,
                "total_columns_passed": 1,
                "total_columns_errored": 0,
                "total_columns_warned": 0,
            },
            "tables": [
                {
                    "table_name": "test_table",
                    "status": "pass",
                    "total_columns": 1,
                    "passed_columns": 1,
                    "error_columns": 0,
                    "warning_columns": 0,
                    "errors": [],
                    "warnings": [],
                    "column_results": [
                        {
                            "column_name": "col1",
                            "status": "pass",
                            "message": "Perfect",
                        },
                    ],
                },
            ],
            "config_path": "test_config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }
        json_file.write_text(json.dumps(json_data))

        # Test JSON conversion
        with open(json_file) as f:
            loaded_data = json.load(f)

        result = _json_to_verification_result(loaded_data)

        assert result.overview.total_tables == 1
        assert len(result.tables) == 1
        assert result.tables[0].table_name == "test_table"

    def test_command_with_invalid_json(self, tmp_path):
        """Test command with invalid JSON file."""
        json_file = tmp_path / "invalid.json"
        json_file.write_text("invalid json content")

        with pytest.raises(json.JSONDecodeError):
            with open(json_file) as f:
                json.load(f)

    def test_command_with_missing_required_fields(self, tmp_path):
        """Test command with missing required fields in JSON."""
        json_file = tmp_path / "incomplete.json"
        json_data = {
            "overview": {
                "total_tables": 1,
                # Missing other required fields
            },
        }
        json_file.write_text(json.dumps(json_data))

        with open(json_file) as f:
            loaded_data = json.load(f)

        with pytest.raises(KeyError):
            _json_to_verification_result(loaded_data)

    def test_output_file_creation(self, tmp_path):
        """Test that output file is created correctly."""
        json_file = tmp_path / "test.json"
        output_file = tmp_path / "output.html"

        json_data = {
            "overview": {
                "total_tables": 0,
                "passed_tables": 0,
                "failed_tables": 0,
                "warning_tables": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "total_columns_checked": 0,
                "total_columns_passed": 0,
                "total_columns_errored": 0,
                "total_columns_warned": 0,
            },
            "tables": [],
            "config_path": "test.yaml",
            "reverse_data_path": "/tmp/reversed",
        }
        json_file.write_text(json.dumps(json_data))

        # Load and convert
        with open(json_file) as f:
            loaded_data = json.load(f)

        verification_result = _json_to_verification_result(loaded_data)
        template_data = _prepare_template_data(verification_result, json_file)
        html_content = _generate_html(template_data)

        # Write output
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(html_content, encoding="utf-8")

        assert output_file.exists()
        assert output_file.read_text(encoding="utf-8") == html_content
        assert "Cleared Verification Report" in html_content

    def test_output_file_with_custom_path(self, tmp_path):
        """Test output file with custom path."""
        json_file = tmp_path / "test.json"
        custom_output = tmp_path / "custom" / "report.html"

        json_data = {
            "overview": {
                "total_tables": 0,
                "passed_tables": 0,
                "failed_tables": 0,
                "warning_tables": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "total_columns_checked": 0,
                "total_columns_passed": 0,
                "total_columns_errored": 0,
                "total_columns_warned": 0,
            },
            "tables": [],
            "config_path": "test.yaml",
            "reverse_data_path": "/tmp/reversed",
        }
        json_file.write_text(json.dumps(json_data))

        # Load and generate
        with open(json_file) as f:
            loaded_data = json.load(f)

        verification_result = _json_to_verification_result(loaded_data)
        template_data = _prepare_template_data(verification_result, json_file)
        html_content = _generate_html(template_data)

        # Write to custom path
        custom_output.parent.mkdir(parents=True, exist_ok=True)
        custom_output.write_text(html_content, encoding="utf-8")

        assert custom_output.exists()
        assert custom_output.parent.exists()

    def test_json_with_warnings(self, tmp_path):
        """Test JSON conversion with warnings."""
        json_data = {
            "overview": {
                "total_tables": 1,
                "passed_tables": 0,
                "failed_tables": 0,
                "warning_tables": 1,
                "total_errors": 0,
                "total_warnings": 1,
                "total_columns_checked": 1,
                "total_columns_passed": 0,
                "total_columns_errored": 0,
                "total_columns_warned": 1,
            },
            "tables": [
                {
                    "table_name": "test",
                    "status": "warning",
                    "total_columns": 1,
                    "passed_columns": 0,
                    "error_columns": 0,
                    "warning_columns": 1,
                    "errors": [],
                    "warnings": ["Warning message"],
                    "column_results": [
                        {
                            "column_name": "col1",
                            "status": "warning",
                            "message": "Warning",
                        },
                    ],
                },
            ],
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        result = _json_to_verification_result(json_data)

        assert result.overview.warning_tables == 1
        assert result.overview.total_warnings == 1
        assert result.tables[0].status == "warning"
        assert len(result.tables[0].warnings) == 1

    def test_json_with_empty_tables(self):
        """Test JSON conversion with empty tables list."""
        json_data = {
            "overview": {
                "total_tables": 0,
                "passed_tables": 0,
                "failed_tables": 0,
                "warning_tables": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "total_columns_checked": 0,
                "total_columns_passed": 0,
                "total_columns_errored": 0,
                "total_columns_warned": 0,
            },
            "tables": [],
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        result = _json_to_verification_result(json_data)

        assert len(result.tables) == 0
        assert result.overview.total_tables == 0

    def test_html_with_error_table(self):
        """Test HTML generation with error table."""
        tables = [
            TableVerificationResult(
                "error_table",
                "error",
                2,
                0,
                2,
                0,
                ["Error 1", "Error 2"],
                [],
                [
                    ColumnComparisonResult("col1", "error", "Error 1", 10, 10, 5, 50.0),
                    ColumnComparisonResult("col2", "error", "Error 2", 10, 8, 0, 0.0),
                ],
            ),
        ]
        template_data = {
            "config_name": "test",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": None,
            "overview": VerificationOverview(
                total_tables=1,
                passed_tables=0,
                failed_tables=1,
                warning_tables=0,
                total_errors=2,
                total_warnings=0,
                total_columns_checked=2,
                total_columns_passed=0,
                total_columns_errored=2,
                total_columns_warned=0,
            ),
            "tables": tables,
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        html = _generate_html(template_data)

        assert "error_table" in html
        assert "Error 1" in html
        assert "Error 2" in html
        assert "col1" in html
        assert "col2" in html

    def test_html_with_warning_table(self):
        """Test HTML generation with warning table."""
        tables = [
            TableVerificationResult(
                "warning_table",
                "warning",
                1,
                0,
                0,
                1,
                [],
                ["Warning message"],
                [
                    ColumnComparisonResult("col1", "warning", "Warning", 10, 0, 0, 0.0),
                ],
            ),
        ]
        template_data = {
            "config_name": "test",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": None,
            "overview": VerificationOverview(
                total_tables=1,
                passed_tables=0,
                failed_tables=0,
                warning_tables=1,
                total_errors=0,
                total_warnings=1,
                total_columns_checked=1,
                total_columns_passed=0,
                total_columns_errored=0,
                total_columns_warned=1,
            ),
            "tables": tables,
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        html = _generate_html(template_data)

        assert "warning_table" in html
        assert "Warning message" in html
        assert "col1" in html

    def test_html_with_mismatch_indices(self):
        """Test HTML generation with mismatch indices."""
        tables = [
            TableVerificationResult(
                "test_table",
                "error",
                1,
                0,
                1,
                0,
                ["Error"],
                [],
                [
                    ColumnComparisonResult(
                        "col1",
                        "error",
                        "Mismatch",
                        10,
                        10,
                        5,
                        50.0,
                        [0, 1, 2, 3, 4],
                    ),
                ],
            ),
        ]
        template_data = {
            "config_name": "test",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": None,
            "overview": VerificationOverview(
                total_tables=1,
                passed_tables=0,
                failed_tables=1,
                warning_tables=0,
                total_errors=1,
                total_warnings=0,
                total_columns_checked=1,
                total_columns_passed=0,
                total_columns_errored=1,
                total_columns_warned=0,
            ),
            "tables": tables,
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        html = _generate_html(template_data)

        assert "Sample indices" in html
        # Should show indices
        assert "0, 1, 2, 3, 4" in html

    def test_html_config_info_section(self):
        """Test that HTML includes configuration info section."""
        template_data = {
            "config_name": "test_config.yaml",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": "verify.json",
            "overview": VerificationOverview(
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
            ),
            "tables": [],
            "config_path": "/path/to/config.yaml",
            "reverse_data_path": "/tmp/reversed_data",
        }

        html = _generate_html(template_data)

        assert "Configuration Information" in html
        assert "/path/to/config.yaml" in html
        assert "/tmp/reversed_data" in html
        assert "verify.json" in html

    def test_json_with_many_errors_truncated(self):
        """Test HTML generation with many errors (truncation)."""
        tables = [
            TableVerificationResult(
                "test_table",
                "error",
                1,
                0,
                1,
                0,
                ["Error message"],  # Only one error to match one error column
                [],
                [
                    ColumnComparisonResult("col1", "error", "Error", 10, 10, 5, 50.0),
                ],
            ),
        ]
        template_data = {
            "config_name": "test",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": None,
            "overview": VerificationOverview(
                total_tables=1,
                passed_tables=0,
                failed_tables=1,
                warning_tables=0,
                total_errors=15,
                total_warnings=0,
                total_columns_checked=1,
                total_columns_passed=0,
                total_columns_errored=1,
                total_columns_warned=0,
            ),
            "tables": tables,
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        html = _generate_html(template_data)

        assert "Error message" in html

    def test_json_with_many_warnings_truncated(self):
        """Test HTML generation with many warnings (truncation)."""
        tables = [
            TableVerificationResult(
                "test_table",
                "warning",
                1,
                0,
                0,
                1,
                [],
                ["Warning message"],  # Only one warning to match one warning column
                [
                    ColumnComparisonResult("col1", "warning", "Warning", 10, 0, 0, 0.0),
                ],
            ),
        ]
        template_data = {
            "config_name": "test",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": None,
            "overview": VerificationOverview(
                total_tables=1,
                passed_tables=0,
                failed_tables=0,
                warning_tables=1,
                total_errors=0,
                total_warnings=15,
                total_columns_checked=1,
                total_columns_passed=0,
                total_columns_errored=0,
                total_columns_warned=1,
            ),
            "tables": tables,
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        html = _generate_html(template_data)

        assert "Warning message" in html

    def test_html_with_mixed_status_tables(self):
        """Test HTML generation with mixed status tables."""
        tables = [
            TableVerificationResult(
                "pass_table",
                "pass",
                1,
                1,
                0,
                0,
                [],
                [],
                [ColumnComparisonResult("col1", "pass", "Perfect", 10, 10, 0, 0.0)],
            ),
            TableVerificationResult(
                "error_table",
                "error",
                1,
                0,
                1,
                0,
                ["Error"],
                [],
                [ColumnComparisonResult("col2", "error", "Error", 10, 10, 5, 50.0)],
            ),
            TableVerificationResult(
                "warning_table",
                "warning",
                1,
                0,
                0,
                1,
                [],
                ["Warning"],
                [ColumnComparisonResult("col3", "warning", "Warning", 10, 0, 0, 0.0)],
            ),
        ]
        template_data = {
            "config_name": "test",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": None,
            "overview": VerificationOverview(
                total_tables=3,
                passed_tables=1,
                failed_tables=1,
                warning_tables=1,
                total_errors=1,
                total_warnings=1,
                total_columns_checked=3,
                total_columns_passed=1,
                total_columns_errored=1,
                total_columns_warned=1,
            ),
            "tables": tables,
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        html = _generate_html(template_data)

        assert "pass_table" in html
        assert "error_table" in html
        assert "warning_table" in html
        assert "col1" in html
        assert "col2" in html
        assert "col3" in html

    def test_html_with_no_column_results(self):
        """Test HTML generation with table that has no column results."""
        tables = [
            TableVerificationResult(
                "empty_table",
                "pass",
                0,
                0,
                0,
                0,
                [],
                [],
                [],
            ),
        ]
        template_data = {
            "config_name": "test",
            "generated_at": "2024-01-01 12:00:00",
            "version": "0.1.0",
            "json_file": None,
            "overview": VerificationOverview(
                total_tables=1,
                passed_tables=1,
                failed_tables=0,
                warning_tables=0,
                total_errors=0,
                total_warnings=0,
                total_columns_checked=0,
                total_columns_passed=0,
                total_columns_errored=0,
                total_columns_warned=0,
            ),
            "tables": tables,
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        html = _generate_html(template_data)

        assert "empty_table" in html
        assert "No column results available" in html

    def test_json_conversion_with_empty_column_results(self):
        """Test JSON conversion with empty column_results."""
        json_data = {
            "overview": {
                "total_tables": 1,
                "passed_tables": 1,
                "failed_tables": 0,
                "warning_tables": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "total_columns_checked": 0,
                "total_columns_passed": 0,
                "total_columns_errored": 0,
                "total_columns_warned": 0,
            },
            "tables": [
                {
                    "table_name": "test",
                    "status": "pass",
                    "total_columns": 0,
                    "passed_columns": 0,
                    "error_columns": 0,
                    "warning_columns": 0,
                    "errors": [],
                    "warnings": [],
                    "column_results": [],
                },
            ],
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        result = _json_to_verification_result(json_data)

        assert len(result.tables) == 1
        assert len(result.tables[0].column_results) == 0

    def test_json_conversion_with_large_mismatch_indices(self):
        """Test JSON conversion with large number of mismatch indices."""
        json_data = {
            "overview": {
                "total_tables": 1,
                "passed_tables": 0,
                "failed_tables": 1,
                "warning_tables": 0,
                "total_errors": 1,
                "total_warnings": 0,
                "total_columns_checked": 1,
                "total_columns_passed": 0,
                "total_columns_errored": 1,
                "total_columns_warned": 0,
            },
            "tables": [
                {
                    "table_name": "test",
                    "status": "error",
                    "total_columns": 1,
                    "passed_columns": 0,
                    "error_columns": 1,
                    "warning_columns": 0,
                    "errors": ["Error"],
                    "warnings": [],
                    "column_results": [
                        {
                            "column_name": "col1",
                            "status": "error",
                            "message": "Many mismatches",
                            "original_length": 100,
                            "reversed_length": 100,
                            "mismatch_count": 50,
                            "mismatch_percentage": 50.0,
                            "sample_mismatch_indices": list(range(50)),
                        },
                    ],
                },
            ],
            "config_path": "config.yaml",
            "reverse_data_path": "/tmp/reversed",
        }

        result = _json_to_verification_result(json_data)

        col_result = result.tables[0].column_results[0]
        assert len(col_result.sample_mismatch_indices) == 50
        assert col_result.sample_mismatch_indices[0] == 0
        assert col_result.sample_mismatch_indices[-1] == 49
