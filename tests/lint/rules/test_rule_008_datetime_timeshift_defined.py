"""Unit tests for rule_datetime_timeshift_defined (cleared-008)."""

from cleared.lint.rules.validation import rule_datetime_timeshift_defined
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


class TestRule008DateTimeTimeshiftDefined:
    """Test rule_datetime_timeshift_defined (cleared-008)."""

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

    def test_datetime_with_valid_timeshift_no_issue(self):
        """Test that no issue is found when DateTimeDeidentifier has valid time_shift."""
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
                ),
            },
        )

        issues = rule_datetime_timeshift_defined(config)
        assert len(issues) == 0

    def test_datetime_without_deid_config(self):
        """Test that issue is found when deid_config is None."""
        # This is a bit tricky since ClearedConfig has a default factory for deid_config
        # But we can test with an explicitly None deid_config if the structure allows it
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),  # Empty but not None
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
                ),
            },
        )

        # Manually set time_shift to None to test
        config.deid_config.time_shift = None

        issues = rule_datetime_timeshift_defined(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-008"
        assert "time_shift is not defined" in issues[0].message

    def test_datetime_without_timeshift(self):
        """Test that issue is found when time_shift is not defined."""
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
                ),
            },
        )

        issues = rule_datetime_timeshift_defined(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-008"
        assert "time_shift is not defined" in issues[0].message

    def test_datetime_without_timeshift_method(self):
        """Test that issue is found when time_shift.method is not defined."""
        # Create a TimeShiftConfig without method (this would normally fail validation)
        # But we can test the linting rule's behavior
        time_shift = TimeShiftConfig(method="random_days", min=-365, max=365)
        time_shift.method = None  # Manually set to None

        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(time_shift=time_shift),
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
                ),
            },
        )

        issues = rule_datetime_timeshift_defined(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-008"
        assert "time_shift.method is not defined" in issues[0].message

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
                ),
            },
        )

        issues = rule_datetime_timeshift_defined(config)
        assert len(issues) == 0

    def test_datetime_in_multiple_tables(self):
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

        issues = rule_datetime_timeshift_defined(config)
        assert len(issues) == 1  # Should only report once
