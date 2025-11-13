"""Unit tests for rule_no_circular_dependencies (cleared-006)."""

from cleared.lint.rules.dependencies import rule_no_circular_dependencies
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
)


class TestRule006NoCircularDependencies:
    """Test rule_no_circular_dependencies (cleared-006)."""

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

    def test_no_circular_dependencies_no_issue(self):
        """Test that no issue is found when there are no circular dependencies."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(name="users", depends_on=[]),
                "orders": TableConfig(name="orders", depends_on=["users"]),
            },
        )

        issues = rule_no_circular_dependencies(config)
        assert len(issues) == 0

    def test_simple_table_circular_dependency(self):
        """Test that simple circular table dependency is detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table_a": TableConfig(name="table_a", depends_on=["table_b"]),
                "table_b": TableConfig(name="table_b", depends_on=["table_a"]),
            },
        )

        issues = rule_no_circular_dependencies(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-006"
        assert "Circular table dependency" in issues[0].message
        assert "table_a" in issues[0].message
        assert "table_b" in issues[0].message

    def test_three_table_circular_dependency(self):
        """Test that three-table circular dependency is detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table_a": TableConfig(name="table_a", depends_on=["table_b"]),
                "table_b": TableConfig(name="table_b", depends_on=["table_c"]),
                "table_c": TableConfig(name="table_c", depends_on=["table_a"]),
            },
        )

        issues = rule_no_circular_dependencies(config)
        assert len(issues) == 1
        assert "Circular table dependency" in issues[0].message
        assert "table_a" in issues[0].message
        assert "table_b" in issues[0].message
        assert "table_c" in issues[0].message

    def test_simple_transformer_circular_dependency(self):
        """Test that simple circular transformer dependency is detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="transformer_a",
                            depends_on=["transformer_b"],
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="transformer_b",
                            depends_on=["transformer_a"],
                            configs={"idconfig": {"name": "id2", "uid": "id2"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_no_circular_dependencies(config)
        assert len(issues) >= 1
        assert any(
            "Circular transformer dependency" in issue.message for issue in issues
        )
        assert any("transformer_a" in issue.message for issue in issues)
        assert any("transformer_b" in issue.message for issue in issues)

    def test_three_transformer_circular_dependency(self):
        """Test that three-transformer circular dependency is detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="transformer_a",
                            depends_on=["transformer_b"],
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="transformer_b",
                            depends_on=["transformer_c"],
                            configs={"idconfig": {"name": "id2", "uid": "id2"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="transformer_c",
                            depends_on=["transformer_a"],
                            configs={"idconfig": {"name": "id3", "uid": "id3"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_no_circular_dependencies(config)
        assert len(issues) >= 1
        assert any(
            "Circular transformer dependency" in issue.message for issue in issues
        )

    def test_mixed_table_and_transformer_circular_dependencies(self):
        """Test that both table and transformer circular dependencies are detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table_a": TableConfig(
                    name="table_a",
                    depends_on=["table_b"],
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="transformer_a",
                            depends_on=["transformer_b"],
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="transformer_b",
                            depends_on=["transformer_a"],
                            configs={"idconfig": {"name": "id2", "uid": "id2"}},
                        ),
                    ],
                ),
                "table_b": TableConfig(name="table_b", depends_on=["table_a"]),
            },
        )

        issues = rule_no_circular_dependencies(config)
        assert len(issues) >= 2
        assert any("Circular table dependency" in issue.message for issue in issues)
        assert any(
            "Circular transformer dependency" in issue.message for issue in issues
        )

    def test_no_duplicate_cycle_reports(self):
        """Test that the same cycle is not reported multiple times."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table_a": TableConfig(name="table_a", depends_on=["table_b"]),
                "table_b": TableConfig(name="table_b", depends_on=["table_a"]),
            },
        )

        issues = rule_no_circular_dependencies(config)
        # Should only report the cycle once, not for each table in the cycle
        assert len(issues) == 1

    def test_valid_chain_no_circular_dependency(self):
        """Test that valid dependency chain is not flagged."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table_a": TableConfig(name="table_a", depends_on=[]),
                "table_b": TableConfig(name="table_b", depends_on=["table_a"]),
                "table_c": TableConfig(name="table_c", depends_on=["table_b"]),
            },
        )

        issues = rule_no_circular_dependencies(config)
        assert len(issues) == 0
