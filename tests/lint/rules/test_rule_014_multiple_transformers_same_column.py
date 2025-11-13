"""Unit tests for rule_multiple_transformers_same_column (cleared-014)."""

from cleared.lint.rules.format import rule_multiple_transformers_same_column
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    TimeShiftConfig,
    FilterConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
)


class TestRule014MultipleTransformersSameColumn:
    """Test rule_multiple_transformers_same_column (cleared-014)."""

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

    def test_single_transformer_no_issue(self):
        """Test that single transformer causes no issues."""
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
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        )
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        assert len(issues) == 0

    def test_multiple_transformers_different_columns_no_issue(self):
        """Test that multiple transformers modifying different columns cause no issues."""
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
                            method="IDDeidentifier",
                            uid="id_transformer",
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
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
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        assert len(issues) == 0

    def test_multiple_iddeidentifier_same_column_warning(self):
        """Test that multiple IDDeidentifier transformers modifying same column generates warning."""
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
                            uid="id_transformer_1",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_1"}
                            },
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer_2",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_2"}
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-014"
        assert issues[0].severity == "warning"
        assert "user_id" in issues[0].message
        assert "id_transformer_1" in issues[0].message
        assert "id_transformer_2" in issues[0].message

    def test_multiple_datetimedeidentifier_same_column_warning(self):
        """Test that multiple DateTimeDeidentifier transformers modifying same column generates warning."""
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
                            uid="datetime_transformer_1",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "reg_date",
                            },
                        ),
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer_2",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "reg_date",
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        assert len(issues) == 1
        assert "reg_date" in issues[0].message
        assert "datetime_transformer_1" in issues[0].message
        assert "datetime_transformer_2" in issues[0].message

    def test_mixed_transformers_same_column_warning(self):
        """Test that mixed transformers modifying same column generates warning."""
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
                            method="IDDeidentifier",
                            uid="id_transformer",
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "user_id",  # Same column as IDDeidentifier
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        assert len(issues) == 1
        assert "user_id" in issues[0].message
        assert "id_transformer" in issues[0].message
        assert "datetime_transformer" in issues[0].message

    def test_transformers_with_filters_ignored(self):
        """Test that transformers with filters are ignored (not checked)."""
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
                            uid="id_transformer_1",
                            filter=FilterConfig(where_condition="status = 'active'"),
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_1"}
                            },
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer_2",
                            filter=FilterConfig(where_condition="status = 'inactive'"),
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_2"}
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        # Transformers with filters should be ignored
        assert len(issues) == 0

    def test_mixed_with_and_without_filters(self):
        """Test that only transformers without filters are checked."""
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
                            uid="id_transformer_1",
                            filter=None,  # No filter
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_1"}
                            },
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer_2",
                            filter=FilterConfig(
                                where_condition="status = 'active'"
                            ),  # Has filter
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_2"}
                            },
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer_3",
                            filter=None,  # No filter
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_3"}
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        # Only id_transformer_1 and id_transformer_3 should be checked (no filters)
        assert len(issues) == 1
        assert "id_transformer_1" in issues[0].message
        assert "id_transformer_3" in issues[0].message
        assert "id_transformer_2" not in issues[0].message  # Has filter, so ignored

    def test_columndropper_not_checked(self):
        """Test that ColumnDropper is not checked (it drops columns, doesn't modify them)."""
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
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                        TransformerConfig(
                            method="ColumnDropper",
                            uid="drop_transformer",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "drop_uid"}
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        # ColumnDropper should not be checked, so no issue
        assert len(issues) == 0

    def test_transformer_without_uid(self):
        """Test that transformer without UID is handled (uses 'unnamed')."""
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
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid=None,  # No UID
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_2"}
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        assert len(issues) == 1
        assert "unnamed" in issues[0].message

    def test_multiple_tables_separate_checks(self):
        """Test that each table is checked separately."""
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
                            uid="user_id_transformer_1",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_1"}
                            },
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="user_id_transformer_2",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_2"}
                            },
                        ),
                    ],
                ),
                "orders": TableConfig(
                    name="orders",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="order_id_transformer_1",
                            configs={
                                "idconfig": {"name": "order_id", "uid": "order_id_1"}
                            },
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="order_id_transformer_2",
                            configs={
                                "idconfig": {"name": "order_id", "uid": "order_id_2"}
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        assert len(issues) == 2
        assert any("users" in issue.message for issue in issues)
        assert any("orders" in issue.message for issue in issues)

    def test_three_transformers_same_column(self):
        """Test that three transformers modifying same column are all reported."""
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
                            uid="id_transformer_1",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_1"}
                            },
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer_2",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_2"}
                            },
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer_3",
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id_3"}
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        assert len(issues) == 1
        assert "id_transformer_1" in issues[0].message
        assert "id_transformer_2" in issues[0].message
        assert "id_transformer_3" in issues[0].message

    def test_missing_config_fields_ignored(self):
        """Test that transformers with missing config fields are ignored."""
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
                            uid="id_transformer_1",
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer_2",
                            configs={},  # Missing idconfig
                        ),
                    ],
                ),
            },
        )

        issues = rule_multiple_transformers_same_column(config)
        # id_transformer_2 has missing config, so it's ignored
        assert len(issues) == 0
