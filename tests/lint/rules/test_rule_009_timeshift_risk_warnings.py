"""Unit tests for rule_timeshift_risk_warnings (cleared-009)."""

from cleared.lint.rules.timeshift import rule_timeshift_risk_warnings
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    TimeShiftConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
)


class TestRule009TimeshiftRiskWarnings:
    """Test rule_timeshift_risk_warnings (cleared-009)."""

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

    def test_shift_by_days_warning(self):
        """Test that shift_by_days generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_days", min=-30, max=30)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_risk_warnings(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-009"
        assert issues[0].severity == "warning"
        assert "shift_by_days" in issues[0].message
        assert "day-of-week" in issues[0].message.lower()

    def test_shift_by_hours_warning(self):
        """Test that shift_by_hours generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_hours", min=-12, max=12)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_risk_warnings(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-009"
        assert issues[0].severity == "warning"
        assert "shift_by_hours" in issues[0].message
        assert "hour-of-day" in issues[0].message.lower()

    def test_random_days_warning(self):
        """Test that random_days generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_risk_warnings(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-009"
        assert issues[0].severity == "warning"
        assert "random_days" in issues[0].message
        assert "day-of-week" in issues[0].message.lower()

    def test_random_hours_warning(self):
        """Test that random_hours generates a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_hours", min=-24, max=24)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_risk_warnings(config)
        assert len(issues) == 1
        assert issues[0].rule == "cleared-009"
        assert issues[0].severity == "warning"
        assert "random_hours" in issues[0].message
        assert "hour-of-day" in issues[0].message.lower()

    def test_shift_by_years_no_warning(self):
        """Test that shift_by_years does not generate a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_years", min=-5, max=5)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_risk_warnings(config)
        assert len(issues) == 0

    def test_shift_by_weeks_no_warning(self):
        """Test that shift_by_weeks does not generate a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_weeks", min=-52, max=52)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_risk_warnings(config)
        assert len(issues) == 0

    def test_shift_by_months_no_warning(self):
        """Test that shift_by_months does not generate a warning."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="shift_by_months", min=-12, max=12)
            ),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_risk_warnings(config)
        assert len(issues) == 0

    def test_no_timeshift_no_warning(self):
        """Test that no warning is generated when time_shift is not defined."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(),  # No time_shift
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_risk_warnings(config)
        assert len(issues) == 0

    def test_none_timeshift_no_warning(self):
        """Test that no warning is generated when time_shift is None."""
        config = ClearedConfig(
            name="test",
            deid_config=DeIDConfig(time_shift=None),
            io=self.valid_io_config,
            tables={},
        )

        issues = rule_timeshift_risk_warnings(config)
        assert len(issues) == 0
