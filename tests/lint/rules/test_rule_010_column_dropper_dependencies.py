"""Unit tests for rule_column_dropper_dependencies (cleared-010)."""

from cleared.lint.rules.dependencies import (
    rule_column_dropper_dependencies,
    _build_execution_order,
)
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


class TestRule010ColumnDropperDependencies:
    """Test rule_column_dropper_dependencies (cleared-010)."""

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

    def test_valid_order_no_issue(self):
        """Test that no issue is found when ColumnDropper comes after transformers that use the column."""
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
                            depends_on=[],
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                        TransformerConfig(
                            method="ColumnDropper",
                            uid="drop_transformer",
                            depends_on=["id_transformer"],
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_column_dropper_dependencies(config)
        assert len(issues) == 0

    def test_column_dropper_before_id_transformer(self):
        """Test that issue is found when ColumnDropper drops column used by later transformer."""
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
                            method="ColumnDropper",
                            uid="drop_transformer",
                            depends_on=[],
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer",
                            depends_on=["drop_transformer"],
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_column_dropper_dependencies(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-010"
        assert "id_transformer" in issues[0].message
        assert "user_id" in issues[0].message
        assert "drop_transformer" in issues[0].message

    def test_column_dropper_before_datetime_transformer_reference_id(self):
        """Test that issue is found when ColumnDropper drops reference ID used by DateTimeDeidentifier."""
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
                            method="ColumnDropper",
                            uid="drop_transformer",
                            depends_on=[],
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer",
                            depends_on=["drop_transformer"],
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "reg_date",
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_column_dropper_dependencies(config)
        assert len(issues) == 1
        assert "datetime_transformer" in issues[0].message
        assert "user_id" in issues[0].message

    def test_column_dropper_before_datetime_transformer_datetime_column(self):
        """Test that issue is found when ColumnDropper drops datetime column used by DateTimeDeidentifier."""
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
                            method="ColumnDropper",
                            uid="drop_transformer",
                            depends_on=[],
                            configs={
                                "idconfig": {"name": "reg_date", "uid": "reg_date"}
                            },
                        ),
                        TransformerConfig(
                            method="DateTimeDeidentifier",
                            uid="datetime_transformer",
                            depends_on=["drop_transformer"],
                            configs={
                                "idconfig": {"name": "user_id", "uid": "user_id"},
                                "datetime_column": "reg_date",
                            },
                        ),
                    ],
                ),
            },
        )

        issues = rule_column_dropper_dependencies(config)
        assert len(issues) == 1
        assert "datetime_transformer" in issues[0].message
        assert "reg_date" in issues[0].message

    def test_multiple_column_droppers(self):
        """Test with multiple ColumnDroppers."""
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
                            method="ColumnDropper",
                            uid="drop_col1",
                            depends_on=[],
                            configs={"idconfig": {"name": "col1", "uid": "col1"}},
                        ),
                        TransformerConfig(
                            method="ColumnDropper",
                            uid="drop_col2",
                            depends_on=["drop_col1"],
                            configs={"idconfig": {"name": "col2", "uid": "col2"}},
                        ),
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="id_transformer",
                            depends_on=["drop_col2"],
                            configs={"idconfig": {"name": "col1", "uid": "col1"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_column_dropper_dependencies(config)
        assert len(issues) == 1
        assert "col1" in issues[0].message

    def test_no_column_dropper_no_issue(self):
        """Test that no issue is found when there are no ColumnDroppers."""
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
                            depends_on=[],
                            configs={"idconfig": {"name": "user_id", "uid": "user_id"}},
                        ),
                    ],
                ),
            },
        )

        issues = rule_column_dropper_dependencies(config)
        assert len(issues) == 0


class TestBuildExecutionOrder:
    """Test _build_execution_order helper function."""

    def test_simple_linear_order(self):
        """Test simple linear dependency chain."""
        transformers = [
            TransformerConfig(method="IDDeidentifier", uid="a", depends_on=[]),
            TransformerConfig(method="IDDeidentifier", uid="b", depends_on=["a"]),
            TransformerConfig(method="IDDeidentifier", uid="c", depends_on=["b"]),
        ]
        transformer_map = {t.uid: t for t in transformers}

        order = _build_execution_order(transformers, transformer_map)
        assert len(order) == 3
        assert order[0].uid == "a"
        assert order[1].uid == "b"
        assert order[2].uid == "c"

    def test_no_dependencies(self):
        """Test transformers with no dependencies."""
        transformers = [
            TransformerConfig(method="IDDeidentifier", uid="a", depends_on=[]),
            TransformerConfig(method="IDDeidentifier", uid="b", depends_on=[]),
            TransformerConfig(method="IDDeidentifier", uid="c", depends_on=[]),
        ]
        transformer_map = {t.uid: t for t in transformers}

        order = _build_execution_order(transformers, transformer_map)
        assert len(order) == 3
        assert all(t.uid in ["a", "b", "c"] for t in order)

    def test_branching_dependencies(self):
        """Test branching dependency structure."""
        transformers = [
            TransformerConfig(method="IDDeidentifier", uid="a", depends_on=[]),
            TransformerConfig(method="IDDeidentifier", uid="b", depends_on=["a"]),
            TransformerConfig(method="IDDeidentifier", uid="c", depends_on=["a"]),
            TransformerConfig(method="IDDeidentifier", uid="d", depends_on=["b", "c"]),
        ]
        transformer_map = {t.uid: t for t in transformers}

        order = _build_execution_order(transformers, transformer_map)
        assert len(order) == 4
        assert order[0].uid == "a"
        assert order[-1].uid == "d"
        # b and c should come before d
        b_idx = next(i for i, t in enumerate(order) if t.uid == "b")
        c_idx = next(i for i, t in enumerate(order) if t.uid == "c")
        d_idx = next(i for i, t in enumerate(order) if t.uid == "d")
        assert b_idx < d_idx
        assert c_idx < d_idx

    def test_transformer_without_uid(self):
        """Test that transformers without UID are handled gracefully."""
        transformers = [
            TransformerConfig(method="IDDeidentifier", uid="a", depends_on=[]),
            TransformerConfig(method="IDDeidentifier", uid=None, depends_on=[]),
            TransformerConfig(method="IDDeidentifier", uid="b", depends_on=["a"]),
        ]
        transformer_map = {t.uid: t for t in transformers if t.uid}

        order = _build_execution_order(transformers, transformer_map)
        # Should include all transformers, even those without UID
        assert len(order) >= 2
        assert any(t.uid == "a" for t in order)
        assert any(t.uid == "b" for t in order)
