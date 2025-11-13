"""Unit tests for rule_datetime_requires_timeshift (cleared-002)."""

from cleared.lint.rules.validation import rule_datetime_requires_timeshift
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    TimeShiftConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
)


class TestRule002DateTimeRequiresTimeshift:
    """Test rule_datetime_requires_timeshift (cleared-002)."""

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

    def test_no_datetime_transformer_no_issue(self):
        """Test that no issue is found when no DateTimeDeidentifier is used."""
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
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        )
                    ],
                )
            },
        )

        issues = rule_datetime_requires_timeshift(config)
        assert len(issues) == 0

    def test_datetime_transformer_with_timeshift_no_issue(self):
        """Test that no issue is found when DateTimeDeidentifier has time_shift."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
            ),
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
                                "datetime_column": "reg_date",
                            },
                        )
                    ],
                )
            },
        )

        issues = rule_datetime_requires_timeshift(config)
        assert len(issues) == 0

    def test_datetime_transformer_without_timeshift_issue(self):
        """Test that issue is found when DateTimeDeidentifier is used without time_shift."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),  # No time_shift
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
                                "datetime_column": "reg_date",
                            },
                        )
                    ],
                )
            },
        )

        issues = rule_datetime_requires_timeshift(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-002"
        assert "DateTimeDeidentifier requires time_shift" in issues[0].message

    def test_datetime_transformer_in_multiple_tables(self):
        """Test that issue is detected when DateTimeDeidentifier is in multiple tables."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),  # No time_shift
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer_1",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "reg_date",
                            },
                        )
                    ],
                ),
                "orders": TableConfig(
                    name="orders",
                    transformers=[
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer_2",
                            configs={
                                "idconfig": {"name": "order_id", "uid": "order_id"},
                                "datetime_column": "order_date",
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_datetime_requires_timeshift(config)
        assert len(issues) == 1  # Should only report once

    def test_mixed_transformers_with_datetime(self):
        """Test with mix of transformers including DateTimeDeidentifier."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),  # No time_shift
            io=self.valid_io_config,
            tables={
                "users": TableConfig(
                    name="users",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer",
                            configs={"idconfig": {"name": "id", "uid": "id"}},
                        ),
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "reg_date",
                            },
                        ),
                    ],
                )
            },
        )

        issues = rule_datetime_requires_timeshift(config)
        assert len(issues) == 1

    def test_datetime_transformer_with_none_timeshift(self):
        """Test that issue is found when time_shift is explicitly None."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(time_shift=None),
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
                                "datetime_column": "reg_date",
                            },
                        )
                    ],
                )
            },
        )

        issues = rule_datetime_requires_timeshift(config)
        assert len(issues) == 1
