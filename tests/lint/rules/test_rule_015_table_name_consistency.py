"""Unit tests for rule_table_name_consistency (cleared-015)."""

from cleared.lint.rules.uniqueness import rule_table_name_consistency
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
)


class TestRule015TableNameConsistency:
    """Test rule_table_name_consistency (cleared-015)."""

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

    def test_unique_names_no_issue(self):
        """Test that unique table names cause no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(name="users", depends_on=[]),
                "orders": TableConfig(name="orders", depends_on=["users"]),
            },
        )

        issues = rule_table_name_consistency(config)
        assert len(issues) == 0

    def test_duplicate_names(self):
        """Test that duplicate table names generate an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table1": TableConfig(name="users", depends_on=[]),
                "table2": TableConfig(name="users", depends_on=[]),  # Duplicate name
            },
        )

        issues = rule_table_name_consistency(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-015"
        assert "users" in issues[0].message
        assert "table1" in issues[0].message
        assert "table2" in issues[0].message
        assert "must be unique" in issues[0].message

    def test_multiple_duplicate_names(self):
        """Test that multiple duplicate names are all detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table1": TableConfig(name="users", depends_on=[]),
                "table2": TableConfig(name="users", depends_on=[]),  # Duplicate
                "table3": TableConfig(name="orders", depends_on=[]),
                "table4": TableConfig(name="orders", depends_on=[]),  # Duplicate
            },
        )

        issues = rule_table_name_consistency(config)
        assert len(issues) == 2
        assert all(issue.rule == "cleared-015" for issue in issues)
        messages = {issue.message for issue in issues}
        assert any(
            "users" in msg and "table1" in msg and "table2" in msg for msg in messages
        )
        assert any(
            "orders" in msg and "table3" in msg and "table4" in msg for msg in messages
        )

    def test_three_tables_same_name(self):
        """Test that three tables with the same name are detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table1": TableConfig(name="users", depends_on=[]),
                "table2": TableConfig(name="users", depends_on=[]),
                "table3": TableConfig(name="users", depends_on=[]),
            },
        )

        issues = rule_table_name_consistency(config)
        assert len(issues) == 1
        assert "users" in issues[0].message
        assert "table1" in issues[0].message
        assert "table2" in issues[0].message
        assert "table3" in issues[0].message

    def test_case_sensitive_duplicates(self):
        """Test that case-sensitive duplicates are detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table1": TableConfig(name="Users", depends_on=[]),
                "table2": TableConfig(
                    name="Users", depends_on=[]
                ),  # Same case, duplicate
            },
        )

        issues = rule_table_name_consistency(config)
        assert len(issues) == 1
        assert "Users" in issues[0].message

    def test_different_case_not_duplicate(self):
        """Test that different case names are not considered duplicates."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table1": TableConfig(name="users", depends_on=[]),
                "table2": TableConfig(name="Users", depends_on=[]),  # Different case
            },
        )

        issues = rule_table_name_consistency(config)
        assert len(issues) == 0  # Different strings, not duplicates

    def test_empty_tables_no_issue(self):
        """Test that empty tables dictionary causes no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_table_name_consistency(config)
        assert len(issues) == 0

    def test_single_table_no_issue(self):
        """Test that single table causes no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "patients": TableConfig(name="patients", depends_on=[]),
            },
        )

        issues = rule_table_name_consistency(config)
        assert len(issues) == 0

    def test_mismatched_key_and_name_no_issue(self):
        """Test that mismatched dictionary key and name field is not an issue."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table_key": TableConfig(name="different_name", depends_on=[]),
            },
        )

        issues = rule_table_name_consistency(config)
        assert len(issues) == 0  # Key-name mismatch is not checked
