"""Unit tests for rule_value_cast_appropriateness (cleared-016)."""

from cleared.lint.rules.transformers import rule_value_cast_appropriateness
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
)


class TestRule016ValueCastAppropriateness:
    """Test rule_value_cast_appropriateness (cleared-016)."""

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

    def test_no_value_cast_no_issue(self):
        """Test that transformers without value_cast cause no issues."""
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
                        ),
                        TransformerConfig(
                            method="ColumnDropper",
                            uid="drop_transformer",
                            configs={"idconfig": {"name": "name"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 0

    def test_iddeidentifier_with_integer_cast_no_issue(self):
        """Test that IDDeidentifier with integer value_cast causes no issues."""
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
                            value_cast="integer",
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 0

    def test_iddeidentifier_with_string_cast_no_issue(self):
        """Test that IDDeidentifier with string value_cast causes no issues."""
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
                            value_cast="string",
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 0

    def test_datetimedeidentifier_with_datetime_cast_no_issue(self):
        """Test that DateTimeDeidentifier with datetime value_cast causes no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "created_at",
                            },
                            value_cast="datetime",
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 0

    def test_columndropper_with_value_cast_error(self):
        """Test that ColumnDropper with value_cast generates an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="ColumnDropper",
                            uid="drop_transformer",
                            configs={"idconfig": {"name": "name"}},
                            value_cast="string",
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-016"
        assert issues[0].severity == "error"
        assert "ColumnDropper" in issues[0].message
        assert "does not support value_cast" in issues[0].message

    def test_iddeidentifier_with_datetime_cast_warning(self):
        """Test that IDDeidentifier with datetime value_cast generates a warning."""
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
                            value_cast="datetime",
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-016"
        assert issues[0].severity == "warning"
        assert "IDDeidentifier" in issues[0].message
        assert "datetime" in issues[0].message
        assert "unusual" in issues[0].message

    def test_datetimedeidentifier_with_integer_cast_warning(self):
        """Test that DateTimeDeidentifier with integer value_cast generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "created_at",
                            },
                            value_cast="integer",
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-016"
        assert issues[0].severity == "warning"
        assert "DateTimeDeidentifier" in issues[0].message
        assert "integer" in issues[0].message
        assert "unusual" in issues[0].message

    def test_datetimedeidentifier_with_float_cast_warning(self):
        """Test that DateTimeDeidentifier with float value_cast generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "created_at",
                            },
                            value_cast="float",
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-016"
        assert issues[0].severity == "warning"
        assert "DateTimeDeidentifier" in issues[0].message
        assert "float" in issues[0].message
        assert "unusual" in issues[0].message

    def test_multiple_issues(self):
        """Test that multiple issues are detected."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="ColumnDropper",
                            uid="drop_transformer",
                            configs={"idconfig": {"name": "name"}},
                            value_cast="string",  # Error: ColumnDropper doesn't support value_cast
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="user_id_transformer",
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                            value_cast="datetime",  # Warning: unusual for IDDeidentifier
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 2
        errors = [issue for issue in issues if issue.severity == "error"]
        warnings = [issue for issue in issues if issue.severity == "warning"]
        assert len(errors) == 1
        assert len(warnings) == 1
        assert "ColumnDropper" in errors[0].message
        assert "IDDeidentifier" in warnings[0].message

    def test_iddeidentifier_with_float_cast_no_warning(self):
        """Test that IDDeidentifier with float value_cast causes no warning (float is acceptable)."""
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
                            value_cast="float",
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 0  # Float is acceptable for IDs (though less common)

    def test_datetimedeidentifier_with_string_cast_no_warning(self):
        """Test that DateTimeDeidentifier with string value_cast causes no warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "created_at",
                            },
                            value_cast="string",
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 0  # String is acceptable (though datetime is preferred)

    def test_pipeline_with_value_cast_error(self):
        """Test that Pipeline transformer with value_cast generates an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="Pipeline",
                            uid="pipeline_transformer",
                            configs={},
                            value_cast="string",
                        ),
                    ],
                ),
            },
        )

        issues = rule_value_cast_appropriateness(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-016"
        assert issues[0].severity == "error"
        assert "Pipeline" in issues[0].message
        assert "does not support value_cast" in issues[0].message
