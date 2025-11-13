"""Unit tests for rule_required_keys (cleared-001)."""

from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    TableConfig,
)
from cleared.lint.rules.validation import rule_required_keys


class TestRule001RequiredKeys:
    """Test rule_required_keys (cleared-001)."""

    def test_all_required_keys_present(self):
        """Test that no issues are found when all required keys are present."""
        io_config = ClearedIOConfig.default()
        config = ClearedConfig(
            name="test_config",
            deid_config=DeIDConfig(),
            io=io_config,
            tables={"table1": TableConfig(name="table1")},
        )

        issues = rule_required_keys(config)
        assert len(issues) == 0

    def test_missing_name_key(self):
        """Test that missing 'name' key is detected."""
        io_config = ClearedIOConfig.default()
        config = ClearedConfig(
            name="",  # Empty name
            deid_config=DeIDConfig(),
            io=io_config,
            tables={"table1": TableConfig(name="table1")},
        )

        issues = rule_required_keys(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-001"
        assert "Missing required key: name" in issues[0].message

    def test_missing_deid_config_key(self):
        """Test that missing 'deid_config' key is detected."""
        io_config = ClearedIOConfig.default()
        config = ClearedConfig(
            name="test_config",
            deid_config=None,  # None deid_config
            io=io_config,
            tables={"table1": TableConfig(name="table1")},
        )

        issues = rule_required_keys(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-001"
        assert "Missing required key: deid_config" in issues[0].message

    def test_missing_io_key(self):
        """Test that missing 'io' key is detected."""
        config = ClearedConfig(
            name="test_config",
            deid_config=DeIDConfig(),
            io=None,  # None io
            tables={"table1": TableConfig(name="table1")},
        )

        issues = rule_required_keys(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-001"
        assert "Missing required key: io" in issues[0].message

    def test_missing_tables_key(self):
        """Test that missing 'tables' key is detected."""
        io_config = ClearedIOConfig.default()
        config = ClearedConfig(
            name="test_config",
            deid_config=DeIDConfig(),
            io=io_config,
            tables={},  # Empty tables
        )

        issues = rule_required_keys(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-001"
        assert "Missing required key: tables" in issues[0].message

    def test_multiple_missing_keys(self):
        """Test that multiple missing keys are all detected."""
        config = ClearedConfig(
            name="test_config",
            deid_config=None,  # Missing
            io=None,  # Missing
            tables={},  # Empty
        )

        issues = rule_required_keys(config)
        assert len(issues) == 3
        missing_keys = {issue.message.split(": ")[1] for issue in issues}
        assert "deid_config" in missing_keys
        assert "io" in missing_keys
        assert "tables" in missing_keys

    def test_empty_tables(self):
        """Test that empty tables dict is detected."""
        io_config = ClearedIOConfig.default()
        config = ClearedConfig(
            name="test_config",
            deid_config=DeIDConfig(),
            io=io_config,
            tables={},  # Empty tables
        )

        issues = rule_required_keys(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-001"
        assert "Missing required key: tables" in issues[0].message
