"""Unit tests for rule_output_paths_system_directories (cleared-018)."""

from cleared.lint.rules.io import rule_output_paths_system_directories
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
)


class TestRule018OutputPathsSystemDirectories:
    """Test rule_output_paths_system_directories (cleared-018)."""

    def test_no_system_directories_no_issue(self):
        """Test that non-system directories cause no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 0

    def test_tmp_directory_warning(self):
        """Test that /tmp directory generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-018"
        assert issues[0].severity == "warning"
        assert "/tmp/output" in issues[0].message
        assert "system directory" in issues[0].message.lower()

    def test_var_directory_warning(self):
        """Test that /var directory generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/var/deid_output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-018"
        assert "/var/deid_output" in issues[0].message

    def test_runtime_path_tmp_warning(self):
        """Test that runtime path in /tmp generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-018"
        assert "/tmp/runtime" in issues[0].message
        assert "Runtime IO path" in issues[0].message

    def test_multiple_system_directories(self):
        """Test that multiple system directories are all detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/var/deid_output"}
                    ),
                ),
                runtime_io_path="/usr/runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 3
        assert all(issue.rule == "cleared-018" for issue in issues)
        assert all(issue.severity == "warning" for issue in issues)
        messages = {issue.message for issue in issues}
        assert any("/tmp/output" in msg for msg in messages)
        assert any("/var/deid_output" in msg for msg in messages)
        assert any("/usr/runtime" in msg for msg in messages)

    def test_exact_system_directory_match(self):
        """Test that exact system directory matches are detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 1
        assert "/tmp" in issues[0].message

    def test_etc_directory_warning(self):
        """Test that /etc directory generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/etc/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 1
        assert "/etc/output" in issues[0].message

    def test_usr_directory_warning(self):
        """Test that /usr directory generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/usr/local/deid"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 1
        assert "/usr/local/deid" in issues[0].message

    def test_sql_io_type_ignored(self):
        """Test that SQL IO type is ignored (only filesystem is checked)."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="sql", configs={"base_path": "/tmp/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="sql", configs={"base_path": "/var/deid_output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 0  # SQL IO type is not checked

    def test_relative_paths_no_issue(self):
        """Test that relative paths are not flagged."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 0

    def test_home_directory_no_issue(self):
        """Test that home directory paths are not flagged."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "~/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 0

    def test_empty_path_no_issue(self):
        """Test that empty paths are not flagged."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(io_type="filesystem", configs={}),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_input"}
                    ),
                    output_config=IOConfig(io_type="filesystem", configs={}),
                ),
                runtime_io_path="",
            ),
        )

        issues = rule_output_paths_system_directories(config)
        assert len(issues) == 0
