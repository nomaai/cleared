"""Unit tests for rule_configuration_complexity (cleared-020)."""

from pathlib import Path
import tempfile
import os

from cleared.lint.rules.complexity import rule_configuration_complexity
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
)


class TestRule020ConfigurationComplexity:
    """Test rule_configuration_complexity (cleared-020)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
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
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

    def test_simple_config_no_issue(self):
        """Test that simple configurations (<= 50 lines) cause no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        # Create a temporary config file with few lines
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("name: test\n")
            f.write("deid_config: {}\n")
            f.write("io: {}\n")
            f.write("tables: {}\n")
            temp_path = Path(f.name)

        try:
            issues = rule_configuration_complexity(temp_path, config)
            assert len(issues) == 0
        finally:
            os.unlink(temp_path)

    def test_complex_config_warning(self):
        """Test that complex configurations (> 50 lines) generate a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        # Create a temporary config file with many lines
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            # Write 60 non-empty lines
            for i in range(60):
                f.write(f"line_{i}: value_{i}\n")
            temp_path = Path(f.name)

        try:
            issues = rule_configuration_complexity(temp_path, config)
            assert len(issues) == 1
            assert issues[0].rule == "cleared-020"
            assert issues[0].severity == "warning"
            assert "60" in issues[0].message
            assert "50" in issues[0].message
            assert (
                "Hydra" in issues[0].message or "defaults" in issues[0].message.lower()
            )
        finally:
            os.unlink(temp_path)

    def test_exactly_50_lines_no_issue(self):
        """Test that exactly 50 lines cause no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        # Create a temporary config file with exactly 50 lines
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            for i in range(50):
                f.write(f"line_{i}: value_{i}\n")
            temp_path = Path(f.name)

        try:
            issues = rule_configuration_complexity(temp_path, config)
            assert len(issues) == 0
        finally:
            os.unlink(temp_path)

    def test_51_lines_warning(self):
        """Test that 51 lines generate a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        # Create a temporary config file with 51 lines
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            for i in range(51):
                f.write(f"line_{i}: value_{i}\n")
            temp_path = Path(f.name)

        try:
            issues = rule_configuration_complexity(temp_path, config)
            assert len(issues) == 1
            assert "51" in issues[0].message
        finally:
            os.unlink(temp_path)

    def test_comments_ignored(self):
        """Test that comment lines are not counted."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        # Create a temporary config file with many comment lines but few actual lines
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("# This is a comment\n")
            f.write("# Another comment\n")
            f.write("name: test\n")
            f.write("# More comments\n")
            f.write("deid_config: {}\n")
            for i in range(100):
                f.write(f"# Comment line {i}\n")
            f.write("tables: {}\n")
            temp_path = Path(f.name)

        try:
            issues = rule_configuration_complexity(temp_path, config)
            assert len(issues) == 0  # Only 3 non-comment lines
        finally:
            os.unlink(temp_path)

    def test_empty_lines_ignored(self):
        """Test that empty lines are not counted."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        # Create a temporary config file with many empty lines but few actual lines
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("name: test\n")
            f.write("\n")
            f.write("\n")
            f.write("deid_config: {}\n")
            for _i in range(100):
                f.write("\n")
            f.write("tables: {}\n")
            temp_path = Path(f.name)

        try:
            issues = rule_configuration_complexity(temp_path, config)
            assert len(issues) == 0  # Only 3 non-empty lines
        finally:
            os.unlink(temp_path)

    def test_mixed_comments_and_empty_lines(self):
        """Test that mixed comments and empty lines are handled correctly."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        # Create a temporary config file with 60 actual lines but many comments/empty lines
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            for i in range(60):
                f.write(f"line_{i}: value_{i}\n")
                f.write(f"# Comment for line {i}\n")
                f.write("\n")
            temp_path = Path(f.name)

        try:
            issues = rule_configuration_complexity(temp_path, config)
            assert len(issues) == 1  # 60 non-empty, non-comment lines
            assert "60" in issues[0].message
        finally:
            os.unlink(temp_path)

    def test_nonexistent_file_no_error(self):
        """Test that nonexistent file doesn't cause an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        # Use a path that doesn't exist
        nonexistent_path = Path("/nonexistent/path/config.yaml")

        # Should not raise an error, just return no issues
        issues = rule_configuration_complexity(nonexistent_path, config)
        assert len(issues) == 0

    def test_very_large_config_warning(self):
        """Test that very large configurations generate appropriate warnings."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        # Create a temporary config file with 200 lines
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            for i in range(200):
                f.write(f"line_{i}: value_{i}\n")
            temp_path = Path(f.name)

        try:
            issues = rule_configuration_complexity(temp_path, config)
            assert len(issues) == 1
            assert "200" in issues[0].message
            assert "50" in issues[0].message
        finally:
            os.unlink(temp_path)

    def test_hydra_suggestion_in_message(self):
        """Test that the warning message suggests using Hydra defaults."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        # Create a temporary config file with many lines
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            for i in range(60):
                f.write(f"line_{i}: value_{i}\n")
            temp_path = Path(f.name)

        try:
            issues = rule_configuration_complexity(temp_path, config)
            assert len(issues) == 1
            message = issues[0].message.lower()
            assert "hydra" in message or "defaults" in message
            assert "modular" in message or "split" in message or "break" in message
        finally:
            os.unlink(temp_path)
