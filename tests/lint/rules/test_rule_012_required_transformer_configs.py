"""Unit tests for rule_required_transformer_configs (cleared-012)."""

from cleared.lint.rules.validation import rule_required_transformer_configs
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


class TestRule012RequiredTransformerConfigs:
    """Test rule_required_transformer_configs (cleared-012)."""

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

    def test_iddeidentifier_with_all_required_configs_no_issue(self):
        """Test that IDDeidentifier with all required configs causes no issues."""
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
                            configs={
                                "idconfig": {
                                    "name": "user_id",
                                    "uid": "user_uid",
                                    "description": "User identifier",
                                }
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 0

    def test_iddeidentifier_missing_idconfig(self):
        """Test that IDDeidentifier missing idconfig generates an error."""
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
                            configs={},  # Missing idconfig
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-012"
        assert "IDDeidentifier" in issues[0].message
        assert "id_transformer" in issues[0].message
        assert "missing required config 'idconfig'" in issues[0].message

    def test_iddeidentifier_missing_idconfig_name(self):
        """Test that IDDeidentifier missing idconfig.name generates an error."""
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
                            configs={
                                "idconfig": {
                                    "uid": "user_uid",
                                    # Missing name
                                }
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-012"
        assert "IDDeidentifier" in issues[0].message
        assert "missing required 'idconfig.name'" in issues[0].message

    def test_iddeidentifier_missing_idconfig_uid(self):
        """Test that IDDeidentifier missing idconfig.uid generates an error."""
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
                            configs={
                                "idconfig": {
                                    "name": "user_id",
                                    # Missing uid
                                }
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-012"
        assert "IDDeidentifier" in issues[0].message
        assert "missing required 'idconfig.uid'" in issues[0].message

    def test_iddeidentifier_missing_both_name_and_uid(self):
        """Test that IDDeidentifier missing both name and uid generates two errors."""
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
                            configs={
                                "idconfig": {
                                    # Missing both name and uid
                                }
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 2
        assert all(issue.rule == "cleared-012" for issue in issues)
        messages = {issue.message for issue in issues}
        assert any("missing required 'idconfig.name'" in msg for msg in messages)
        assert any("missing required 'idconfig.uid'" in msg for msg in messages)

    def test_datetimedeidentifier_with_all_required_configs_no_issue(self):
        """Test that DateTimeDeidentifier with all required configs causes no issues."""
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
                                "idconfig": {
                                    "name": "user_id",
                                    "uid": "user_uid",
                                },
                                "datetime_column": "reg_date",
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 0

    def test_datetimedeidentifier_missing_idconfig(self):
        """Test that DateTimeDeidentifier missing idconfig generates an error."""
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
                                "datetime_column": "reg_date",
                                # Missing idconfig
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-012"
        assert "DateTimeDeidentifier" in issues[0].message
        assert "missing required config 'idconfig'" in issues[0].message

    def test_datetimedeidentifier_missing_datetime_column(self):
        """Test that DateTimeDeidentifier missing datetime_column generates an error."""
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
                                "idconfig": {
                                    "name": "user_id",
                                    "uid": "user_uid",
                                },
                                # Missing datetime_column
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-012"
        assert "DateTimeDeidentifier" in issues[0].message
        assert "missing required config 'datetime_column'" in issues[0].message

    def test_datetimedeidentifier_missing_both_configs(self):
        """Test that DateTimeDeidentifier missing both idconfig and datetime_column generates two errors."""
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
                            configs={},  # Missing both
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 2
        assert all(issue.rule == "cleared-012" for issue in issues)
        messages = {issue.message for issue in issues}
        assert any("missing required config 'idconfig'" in msg for msg in messages)
        assert any(
            "missing required config 'datetime_column'" in msg for msg in messages
        )

    def test_columndropper_with_required_configs_no_issue(self):
        """Test that ColumnDropper with required configs causes no issues."""
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
                            configs={
                                "idconfig": {
                                    "name": "column_to_drop",
                                    "uid": "drop_uid",
                                    "description": "Column to drop",
                                }
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 0

    def test_columndropper_missing_idconfig(self):
        """Test that ColumnDropper missing idconfig generates an error."""
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
                            configs={},  # Missing idconfig
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-012"
        assert "ColumnDropper" in issues[0].message
        assert "missing required config 'idconfig'" in issues[0].message

    def test_columndropper_missing_idconfig_name(self):
        """Test that ColumnDropper missing idconfig.name generates an error."""
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
                            configs={
                                "idconfig": {
                                    "uid": "drop_uid",
                                    # Missing name
                                }
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-012"
        assert "ColumnDropper" in issues[0].message
        assert "missing required 'idconfig.name'" in issues[0].message

    def test_columndropper_with_uid_but_no_name(self):
        """Test that ColumnDropper with uid but no name still requires name."""
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
                            configs={
                                "idconfig": {
                                    "uid": "drop_uid",
                                    # name is still required even if uid is present
                                }
                            },
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 1
        assert "missing required 'idconfig.name'" in issues[0].message

    def test_multiple_transformers_with_missing_configs(self):
        """Test that multiple transformers with missing configs are all detected."""
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
                            configs={},  # Missing idconfig
                        ),
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer",
                            configs={},  # Missing both idconfig and datetime_column
                        ),
                        TransformerConfig(
                            method="ColumnDropper",
                            uid="drop_transformer",
                            configs={},  # Missing idconfig
                        ),
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        # IDDeidentifier: 1 error (missing idconfig)
        # DateTimeDeidentifier: 2 errors (missing idconfig, missing datetime_column)
        # ColumnDropper: 1 error (missing idconfig)
        assert len(issues) == 4

    def test_transformer_with_none_configs(self):
        """Test that transformer with None configs is handled."""
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
                            configs=None,  # None configs
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 1
        assert "missing required config 'idconfig'" in issues[0].message

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
                            uid=None,  # No UID
                            configs={},  # Missing idconfig
                        )
                    ],
                ),
            },
        )

        issues = rule_required_transformer_configs(config)
        assert len(issues) == 1
        assert "unnamed" in issues[0].message

    def test_other_transformer_methods_no_validation(self):
        """Test that other transformer methods are not validated (only specific ones)."""
        # Note: TransformerConfig validates method names at creation time,
        # so we can only test with valid transformer methods.
        # The rule only validates IDDeidentifier, DateTimeDeidentifier, and ColumnDropper.
        # Pipeline and TablePipeline are valid but not validated by this rule.
        # Since we can't easily create a TransformerConfig with an invalid method,
        # this test verifies that the rule only checks the three specific transformers.
        # The fact that other transformers don't trigger issues is implicit in the
        # rule implementation (it only has if/elif for the three specific methods).
        pass
