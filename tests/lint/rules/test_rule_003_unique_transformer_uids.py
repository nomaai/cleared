"""Unit tests for rule_unique_transformer_uids (cleared-003)."""

from cleared.lint.rules.uniqueness import rule_unique_transformer_uids
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
)


class TestRule003UniqueTransformerUids:
    """Test rule_unique_transformer_uids (cleared-003)."""

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

    def test_unique_uids_no_issue(self):
        """Test that no issue is found when all transformer UIDs are unique."""
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
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        )
                    ],
                ),
                "orders": TableConfig(
                    name="orders",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="order_id_transformer",
                            configs={
                                "idconfig": {"name": "order_id", "uid": "order_id"}
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_unique_transformer_uids(config)
        assert len(issues) == 0

    def test_duplicate_uid_across_tables(self):
        """Test that duplicate UID across tables is detected."""
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
                            uid="shared_transformer",
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        )
                    ],
                ),
                "orders": TableConfig(
                    name="orders",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="shared_transformer",
                            configs={
                                "idconfig": {"name": "order_id", "uid": "order_id"}
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_unique_transformer_uids(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-003"
        assert "shared_transformer" in issues[0].message
        assert "users" in issues[0].message
        assert "orders" in issues[0].message

    def test_duplicate_uid_in_three_tables(self):
        """Test that duplicate UID in three tables is detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table1": TableConfig(
                    name="table1",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="duplicate_uid",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        )
                    ],
                ),
                "table2": TableConfig(
                    name="table2",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="duplicate_uid",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        )
                    ],
                ),
                "table3": TableConfig(
                    name="table3",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="duplicate_uid",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        )
                    ],
                ),
            },
        )

        issues = rule_unique_transformer_uids(config)
        assert len(issues) == 1
        assert "duplicate_uid" in issues[0].message
        assert "table1" in issues[0].message
        assert "table2" in issues[0].message
        assert "table3" in issues[0].message

    def test_multiple_duplicate_uids(self):
        """Test that multiple duplicate UIDs are all detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "table1": TableConfig(
                    name="table1",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="duplicate1",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="duplicate2",
                            configs={"idconfig": {"name": "id2", "uid": "id2"}},
                        ),
                    ],
                ),
                "table2": TableConfig(
                    name="table2",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="duplicate1",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="duplicate2",
                            configs={"idconfig": {"name": "id2", "uid": "id2"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_unique_transformer_uids(config)
        assert len(issues) == 2
        uids_in_messages = {issue.message for issue in issues}
        assert any("duplicate1" in msg for msg in uids_in_messages)
        assert any("duplicate2" in msg for msg in uids_in_messages)

    def test_none_uid_ignored(self):
        """Test that transformers with None UID are ignored."""
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
                            uid=None,  # None UID
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        )
                    ],
                ),
            },
        )

        issues = rule_unique_transformer_uids(config)
        assert len(issues) == 0

    def test_empty_string_uid(self):
        """Test that empty string UID is treated as valid (though not recommended)."""
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
                            uid="",  # Empty string
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        )
                    ],
                ),
            },
        )

        issues = rule_unique_transformer_uids(config)
        # Empty string is falsy, so it should be ignored
        assert len(issues) == 0
