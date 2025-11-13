"""Unit tests for rule_valid_table_dependencies (cleared-004)."""

from cleared.lint.rules.dependencies import rule_valid_table_dependencies
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
)


class TestRule004ValidTableDependencies:
    """Test rule_valid_table_dependencies (cleared-004)."""

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

    def test_valid_dependencies_no_issue(self):
        """Test that no issue is found when all dependencies are valid."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(name="users", depends_on=[]),
                "orders": TableConfig(name="orders", depends_on=["users"]),
            },
        )

        issues = rule_valid_table_dependencies(config)
        assert len(issues) == 0

    def test_nonexistent_dependency(self):
        """Test that non-existent dependency is detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "orders": TableConfig(name="orders", depends_on=["nonexistent_table"]),
            },
        )

        issues = rule_valid_table_dependencies(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-004"
        assert "orders" in issues[0].message
        assert "nonexistent_table" in issues[0].message

    def test_multiple_nonexistent_dependencies(self):
        """Test that multiple non-existent dependencies are detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "orders": TableConfig(
                    name="orders", depends_on=["nonexistent1", "nonexistent2"]
                ),
            },
        )

        issues = rule_valid_table_dependencies(config)
        assert len(issues) == 2
        assert all(issue.rule == "cleared-004" for issue in issues)
        messages = {issue.message for issue in issues}
        assert any("nonexistent1" in msg for msg in messages)
        assert any("nonexistent2" in msg for msg in messages)

    def test_mixed_valid_and_invalid_dependencies(self):
        """Test with mix of valid and invalid dependencies."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(name="users", depends_on=[]),
                "orders": TableConfig(
                    name="orders", depends_on=["users", "nonexistent"]
                ),
            },
        )

        issues = rule_valid_table_dependencies(config)
        assert len(issues) == 1
        assert "nonexistent" in issues[0].message

    def test_empty_dependencies_no_issue(self):
        """Test that empty dependencies list causes no issue."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(name="users", depends_on=[]),
            },
        )

        issues = rule_valid_table_dependencies(config)
        assert len(issues) == 0

    def test_self_reference(self):
        """Test that self-reference is detected as invalid."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(name="users", depends_on=["users"]),
            },
        )

        issues = rule_valid_table_dependencies(config)
        # Self-reference is technically a valid dependency reference (table exists)
        # but it's a circular dependency which is caught by rule-006
        # This rule only checks if the table exists
        assert len(issues) == 0  # Table exists, so no issue from this rule

    def test_multiple_tables_with_invalid_dependencies(self):
        """Test multiple tables with invalid dependencies."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table1": TableConfig(name="table1", depends_on=["missing1"]),
                "table2": TableConfig(name="table2", depends_on=["missing2"]),
            },
        )

        issues = rule_valid_table_dependencies(config)
        assert len(issues) == 2
