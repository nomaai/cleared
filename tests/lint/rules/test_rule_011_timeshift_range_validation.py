"""Unit tests for rule_timeshift_range_validation (cleared-011)."""

from cleared.lint.rules.timeshift import rule_timeshift_range_validation
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    TimeShiftConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
)


class TestRule011TimeshiftRangeValidation:
    """Test rule_timeshift_range_validation (cleared-011)."""

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

    def test_valid_range_no_issue(self):
        """Test that valid range (min < max) causes no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 0

    def test_equal_min_max_no_issue(self):
        """Test that equal min and max (min == max) causes no issues."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=0, max=0)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 0

    def test_min_greater_than_max_error(self):
        """Test that min > max generates an error."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=365, max=-365)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-011"
        assert issues[0].severity == "error"
        assert "min (365) > max (-365)" in issues[0].message
        assert "min must be less than or equal to max" in issues[0].message

    def test_entirely_negative_range_warning(self):
        """Test that entirely negative range (both min and max negative) generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_days", min=-365, max=-1)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-011"
        assert issues[0].severity == "warning"
        assert "entirely negative" in issues[0].message.lower()
        assert "min: -365" in issues[0].message
        assert "max: -1" in issues[0].message
        assert "shift dates backward" in issues[0].message.lower()

    def test_negative_min_positive_max_no_warning(self):
        """Test that negative min with positive max does not generate negative range warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 0

    def test_positive_range_no_warning(self):
        """Test that entirely positive range does not generate warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=1, max=365)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 0

    def test_zero_range_no_warning(self):
        """Test that zero range (min=0, max=0) does not generate warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=0, max=0)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 0

    def test_min_none_no_validation(self):
        """Test that None min value skips validation."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=None, max=365)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 0

    def test_max_none_no_validation(self):
        """Test that None max value skips validation."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=-365, max=None)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 0

    def test_both_none_no_validation(self):
        """Test that both None values skip validation."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=None, max=None)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 0

    def test_no_timeshift_no_validation(self):
        """Test that no time_shift skips validation."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),  # No time_shift
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 0

    def test_min_greater_than_max_with_negative_values(self):
        """Test that min > max error is detected even with negative values."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=-1, max=-365)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 1
        assert issues[0].severity == "error"
        assert "min (-1) > max (-365)" in issues[0].message

    def test_entirely_negative_range_different_methods(self):
        """Test that entirely negative range warning applies to all methods."""
        methods = [
            "shift_by_days",
            "shift_by_hours",
            "shift_by_weeks",
            "shift_by_months",
            "shift_by_years",
            "random_days",
            "random_hours",
        ]

        for method in methods:
            config = ClearedConfig(
                name="test",
                deid_config=DeIDConfig(
                    time_shift=TimeShiftConfig(method=method, min=-100, max=-1)
                ),
                io=self.valid_io_config,
                tables={},
            )

            issues = rule_timeshift_range_validation(config)
            assert len(issues) == 1
            assert issues[0].severity == "warning"
            assert "entirely negative" in issues[0].message.lower()

    def test_error_takes_precedence_over_warning(self):
        """Test that when min > max, only error is reported (not warning)."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=100, max=-100)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 1
        assert issues[0].severity == "error"
        # Should not have warning about negative range when there's an error
        assert "entirely negative" not in issues[0].message.lower()

    def test_large_negative_range_warning(self):
        """Test warning with large negative range."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_days", min=-1000, max=-500)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 1
        assert issues[0].severity == "warning"
        assert "min: -1000" in issues[0].message
        assert "max: -500" in issues[0].message

    def test_small_negative_range_warning(self):
        """Test warning with small negative range."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_hours", min=-24, max=-1)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_range_validation(config)
        assert len(issues) == 1
        assert issues[0].severity == "warning"
        assert "min: -24" in issues[0].message
        assert "max: -1" in issues[0].message
