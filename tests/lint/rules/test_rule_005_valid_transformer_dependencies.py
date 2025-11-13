"""Unit tests for rule_valid_transformer_dependencies (cleared-005)."""

from cleared.lint.rules.dependencies import rule_valid_transformer_dependencies
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
)


class TestRule005ValidTransformerDependencies:
    """Test rule_valid_transformer_dependencies (cleared-005)."""

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
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer",
                            depends_on=[],
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                        TransformerConfig(
                            method="ColumnDropper",
                            uid="drop_transformer",
                            depends_on=["id_transformer"],
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_valid_transformer_dependencies(config)
        assert len(issues) == 0

    def test_nonexistent_dependency(self):
        """Test that non-existent dependency is detected."""
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
                            uid="id_transformer",
                            depends_on=["nonexistent_transformer"],
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_valid_transformer_dependencies(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-005"
        assert "id_transformer" in issues[0].message
        assert "nonexistent_transformer" in issues[0].message
        assert "users" in issues[0].message

    def test_dependency_from_different_table(self):
        """Test that dependency from different table is detected as invalid."""
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
                            uid="user_id_transformer",
                            depends_on=[],
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                    ],
                ),
                "orders": TableConfig(
                    name="orders",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="order_id_transformer",
                            depends_on=["user_id_transformer"],  # From different table
                            configs={
                                "idconfig": {"name": "order_id", "uid": "order_id"}
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_valid_transformer_dependencies(config)
        assert len(issues) == 1
        assert "order_id_transformer" in issues[0].message
        assert "user_id_transformer" in issues[0].message
        assert "orders" in issues[0].message

    def test_multiple_nonexistent_dependencies(self):
        """Test that multiple non-existent dependencies are detected."""
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
                            uid="id_transformer",
                            depends_on=["missing1", "missing2"],
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_valid_transformer_dependencies(config)
        assert len(issues) == 2
        assert all(issue.rule == "cleared-005" for issue in issues)
        messages = {issue.message for issue in issues}
        assert any("missing1" in msg for msg in messages)
        assert any("missing2" in msg for msg in messages)

    def test_empty_dependencies_no_issue(self):
        """Test that empty dependencies list causes no issue."""
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
                            uid="id_transformer",
                            depends_on=[],
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_valid_transformer_dependencies(config)
        assert len(issues) == 0

    def test_transformer_without_uid(self):
        """Test that transformer without UID is handled gracefully."""
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
                            uid=None,  # No UID
                            depends_on=[],
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                        TransformerConfig(
                            method="ColumnDropper",
                            uid="drop_transformer",
                            depends_on=[],  # Can't depend on transformer without UID
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_valid_transformer_dependencies(config)
        assert len(issues) == 0

    def test_self_reference(self):
        """Test that self-reference is detected as invalid."""
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
                            uid="id_transformer",
                            depends_on=["id_transformer"],  # Self-reference
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_valid_transformer_dependencies(config)
        # Self-reference is technically a valid dependency reference (transformer exists)
        # but it's a circular dependency which is caught by rule-006
        # This rule only checks if the transformer exists in the same table
        assert len(issues) == 0  # Transformer exists, so no issue from this rule
