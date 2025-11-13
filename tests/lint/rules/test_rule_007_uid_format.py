"""Unit tests for rule_uid_format (cleared-007)."""

from cleared.lint.rules.format import rule_uid_format
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
)


class TestRule007UidFormat:
    """Test rule_uid_format (cleared-007)."""

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

    def test_valid_uid_format_no_issue(self):
        """Test that valid UID formats cause no issues."""
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
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="transformer_123",
                            configs={"idconfig": {"name": "id2", "uid": "id2"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 0

    def test_single_character_uid(self):
        """Test that single character UID is valid."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "a": TableConfig(
                    name="a",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="x",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 0

    def test_table_name_with_uppercase(self):
        """Test that table name with uppercase is invalid."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "InvalidTableName": TableConfig(
                    name="InvalidTableName",
                    transformers=[],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-007"
        assert "InvalidTableName" in issues[0].message

    def test_table_name_starting_with_underscore(self):
        """Test that table name starting with underscore is invalid."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "_invalid": TableConfig(
                    name="_invalid",
                    transformers=[],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 1
        assert "_invalid" in issues[0].message

    def test_table_name_ending_with_underscore(self):
        """Test that table name ending with underscore is invalid."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "invalid_": TableConfig(
                    name="invalid_",
                    transformers=[],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 1
        assert "invalid_" in issues[0].message

    def test_transformer_uid_with_uppercase(self):
        """Test that transformer UID with uppercase is invalid."""
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
                            uid="InvalidUID",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-007"
        assert "InvalidUID" in issues[0].message
        assert "users" in issues[0].message

    def test_transformer_uid_starting_with_underscore(self):
        """Test that transformer UID starting with underscore is invalid."""
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
                            uid="_invalid_uid",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 1
        assert "_invalid_uid" in issues[0].message

    def test_transformer_uid_ending_with_underscore(self):
        """Test that transformer UID ending with underscore is invalid."""
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
                            uid="invalid_uid_",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 1
        assert "invalid_uid_" in issues[0].message

    def test_transformer_uid_with_hyphen(self):
        """Test that transformer UID with hyphen is invalid."""
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
                            uid="invalid-uid",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 1
        assert "invalid-uid" in issues[0].message

    def test_transformer_uid_with_spaces(self):
        """Test that transformer UID with spaces is invalid."""
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
                            uid="invalid uid",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 1
        assert "invalid uid" in issues[0].message

    def test_multiple_invalid_uids(self):
        """Test that multiple invalid UIDs are all detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "InvalidTable": TableConfig(
                    name="InvalidTable",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="_invalid_",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 2
        assert all(issue.rule == "cleared-007" for issue in issues)

    def test_transformer_with_none_uid(self):
        """Test that transformer with None UID is ignored."""
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
                            uid=None,
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_uid_format(config)
        assert len(issues) == 0
