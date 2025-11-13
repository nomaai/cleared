"""Unit tests for rule_io_configuration_validation (cleared-013)."""

from cleared.lint.rules.io import rule_io_configuration_validation
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
)


class TestRule013IOConfigurationValidation:
    """Test rule_io_configuration_validation (cleared-013)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem",
                    configs={"base_path": "/tmp/input", "file_format": "csv"},
                ),
                output_config=IOConfig(
                    io_type="filesystem",
                    configs={"base_path": "/tmp/output", "file_format": "csv"},
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

    def test_valid_io_config_no_issue(self):
        """Test that valid IO configuration causes no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        assert len(issues) == 0

    def test_invalid_io_type_in_data_input(self):
        """Test that invalid io_type in data.input_config generates an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="invalid_type", configs={"base_path": "/tmp/input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-013"
        assert "invalid io_type" in issues[0].message.lower()
        assert "data.input_config" in issues[0].message

    def test_invalid_io_type_in_data_output(self):
        """Test that invalid io_type in data.output_config generates an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/input"}
                    ),
                    output_config=IOConfig(
                        io_type="invalid_type", configs={"base_path": "/tmp/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        assert len(issues) == 1
        assert "data.output_config" in issues[0].message

    def test_missing_base_path_in_filesystem_input(self):
        """Test that missing base_path in filesystem input generates an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={}
                    ),  # Missing base_path
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-013"
        assert "Missing required 'base_path'" in issues[0].message
        assert "data.input_config" in issues[0].message

    def test_missing_base_path_in_filesystem_output(self):
        """Test that missing base_path in filesystem output generates an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={}
                    ),  # Missing base_path
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        assert len(issues) == 1
        assert "data.output_config" in issues[0].message

    def test_invalid_file_format(self):
        """Test that invalid file_format generates an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={
                            "base_path": "/tmp/input",
                            "file_format": "invalid_format",
                        },
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-013"
        assert "Invalid file_format" in issues[0].message
        assert "invalid_format" in issues[0].message

    def test_valid_file_formats(self):
        """Test that all valid file formats are accepted."""
        valid_formats = ["csv", "parquet", "json", "excel", "xlsx", "xls", "pickle"]

        for file_format in valid_formats:
            config = ClearedConfig(
                name="test",
                deid_config=DeIDConfig(),
                io=ClearedIOConfig(
                    data=PairedIOConfig(
                        input_config=IOConfig(
                            io_type="filesystem",
                            configs={
                                "base_path": "/tmp/input",
                                "file_format": file_format,
                            },
                        ),
                        output_config=IOConfig(
                            io_type="filesystem", configs={"base_path": "/tmp/output"}
                        ),
                    ),
                    deid_ref=PairedIOConfig(
                        input_config=IOConfig(
                            io_type="filesystem",
                            configs={"base_path": "/tmp/deid_input"},
                        ),
                        output_config=IOConfig(
                            io_type="filesystem",
                            configs={"base_path": "/tmp/deid_output"},
                        ),
                    ),
                    runtime_io_path="/tmp/runtime",
                ),
                tables={},
            )

            issues = rule_io_configuration_validation(config)
            # Should not have file_format errors (may have other issues like same path)
            file_format_issues = [
                issue for issue in issues if "file_format" in issue.message.lower()
            ]
            assert len(file_format_issues) == 0

    def test_same_input_output_path_warning(self):
        """Test that same input and output path generates a warning."""
        same_path = "/tmp/data"
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": same_path, "file_format": "csv"},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": same_path, "file_format": "csv"},
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-013"
        assert issues[0].severity == "warning"
        assert "same" in issues[0].message.lower()
        assert same_path in issues[0].message
        assert "data loss" in issues[0].message.lower()

    def test_different_input_output_paths_no_warning(self):
        """Test that different input and output paths cause no warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp/input", "file_format": "csv"},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp/output", "file_format": "csv"},
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        same_path_issues = [
            issue
            for issue in issues
            if "same" in issue.message.lower() and "path" in issue.message.lower()
        ]
        assert len(same_path_issues) == 0

    def test_sql_io_type_no_base_path_required(self):
        """Test that SQL io_type doesn't require base_path."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="sql",
                        configs={"database_url": "postgresql://localhost/db"},
                    ),
                    output_config=IOConfig(
                        io_type="sql",
                        configs={"database_url": "postgresql://localhost/db"},
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        base_path_issues = [
            issue for issue in issues if "base_path" in issue.message.lower()
        ]
        assert len(base_path_issues) == 0

    def test_deid_ref_missing_base_path(self):
        """Test that missing base_path in deid_ref generates an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={}
                    ),  # Missing base_path
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        assert len(issues) >= 1
        assert any("deid_ref.input_config" in issue.message for issue in issues)

    def test_missing_file_format_no_error(self):
        """Test that missing file_format doesn't generate an error (it's optional)."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/tmp/input"},  # No file_format
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        file_format_issues = [
            issue for issue in issues if "file_format" in issue.message.lower()
        ]
        assert len(file_format_issues) == 0

    def test_same_path_different_io_types_no_warning(self):
        """Test that same path with different io_types doesn't generate warning."""
        same_path = "/tmp/data"
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": same_path}
                    ),
                    output_config=IOConfig(
                        io_type="sql",
                        configs={"database_url": "postgresql://localhost/db"},
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
            tables={},
        )

        issues = rule_io_configuration_validation(config)
        same_path_issues = [
            issue
            for issue in issues
            if "same" in issue.message.lower() and "path" in issue.message.lower()
        ]
        assert len(same_path_issues) == 0
