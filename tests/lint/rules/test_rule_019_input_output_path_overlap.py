"""Unit tests for rule_input_output_path_overlap (cleared-019)."""

from cleared.lint.rules.io import rule_input_output_path_overlap
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
)


class TestRule019InputOutputPathOverlap:
    """Test rule_input_output_path_overlap (cleared-019)."""

    def test_no_overlap_no_issue(self):
        """Test that non-overlapping paths cause no issues."""
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

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 0

    def test_exact_match_data_paths_warning(self):
        """Test that exact match between data input and output generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data"}
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

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-019"
        assert issues[0].severity == "warning"
        assert "./data" in issues[0].message
        assert "overlap" in issues[0].message.lower()

    def test_output_subdirectory_of_input_warning(self):
        """Test that output as subdirectory of input generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data/output"}
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

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 1
        assert "./data" in issues[0].message
        assert "./data/output" in issues[0].message

    def test_input_subdirectory_of_output_warning(self):
        """Test that input as subdirectory of output generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data/input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data"}
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

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 1
        assert "./data/input" in issues[0].message
        assert "./data" in issues[0].message

    def test_deid_ref_paths_overlap_warning(self):
        """Test that overlapping deid_ref paths generate a warning."""
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
                        io_type="filesystem", configs={"base_path": "./deid"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid/output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 1
        assert "DeID reference" in issues[0].message
        assert "./deid" in issues[0].message

    def test_data_input_overlaps_deid_output_warning(self):
        """Test that data input overlapping with deid_ref output generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data"}
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
                        io_type="filesystem",
                        configs={"base_path": "./data/deid_output"},
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 1
        assert "Data input path" in issues[0].message
        assert "DeID reference output path" in issues[0].message
        assert "./data" in issues[0].message

    def test_data_output_overlaps_deid_input_warning(self):
        """Test that data output overlapping with deid_ref input generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid/input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid_output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 1
        assert "Data output path" in issues[0].message
        assert "DeID reference input path" in issues[0].message
        assert "./deid" in issues[0].message

    def test_multiple_overlaps(self):
        """Test that multiple overlaps are all detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./deid/output"}
                    ),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 2
        assert all(issue.rule == "cleared-019" for issue in issues)
        assert all(issue.severity == "warning" for issue in issues)

    def test_trailing_slash_handled(self):
        """Test that trailing slashes are handled correctly."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data/"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data"}
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

        issues = rule_input_output_path_overlap(config)
        assert (
            len(issues) == 1
        )  # Should detect overlap despite trailing slash difference

    def test_sql_io_type_ignored(self):
        """Test that SQL IO type is ignored (only filesystem is checked)."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "./data"}
                    ),
                    output_config=IOConfig(
                        io_type="sql", configs={"base_path": "./data"}
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

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 0  # SQL IO type is not checked

    def test_empty_paths_no_issue(self):
        """Test that empty paths cause no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(io_type="filesystem", configs={}),
                    output_config=IOConfig(io_type="filesystem", configs={}),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(io_type="filesystem", configs={}),
                    output_config=IOConfig(io_type="filesystem", configs={}),
                ),
                runtime_io_path="./runtime",
            ),
        )

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 0

    def test_absolute_paths_overlap(self):
        """Test that absolute paths are checked for overlap."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/home/user/data"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/home/user/data/output"},
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

        issues = rule_input_output_path_overlap(config)
        assert len(issues) == 1
        assert "/home/user/data" in issues[0].message
