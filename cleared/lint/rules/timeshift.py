"""Time shift-related linting rules for Cleared configuration files."""

from cleared.config.structure import ClearedConfig
from cleared.lint.types import LintIssue


def rule_timeshift_range_validation(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-011: Validate time shift range configuration.

    - Check that min ≤ max for time shift configurations (error)
    - Warn if ranges are entirely negative (both min and max are negative)

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    if not config.deid_config.time_shift:
        return issues

    time_shift = config.deid_config.time_shift
    min_val = time_shift.min
    max_val = time_shift.max

    # Check if both min and max are provided
    if min_val is not None and max_val is not None:
        # Error: min > max
        if min_val > max_val:
            issues.append(
                LintIssue(
                    "cleared-011",
                    f"Time shift range is invalid: min ({min_val}) > max ({max_val}). "
                    f"min must be less than or equal to max.",
                    severity="error",
                )
            )
        # Warning: both min and max are negative (entire range is negative)
        elif min_val < 0 and max_val < 0:
            issues.append(
                LintIssue(
                    "cleared-011",
                    f"Time shift range is entirely negative (min: {min_val}, max: {max_val}). "
                    f"This will always shift dates backward in time, which may not be the intended behavior.",
                    severity="warning",
                )
            )

    return issues


def rule_timeshift_risk_warnings(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-009: Warn about risks associated with specific time_shift methods.

    - shift_by_days: Warns about day-of-week pattern changes
    - shift_by_hours: Warns about hour-of-day pattern changes (night vs day shift)

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects (warnings)

    """
    issues: list[LintIssue] = []

    if not config.deid_config.time_shift:
        return issues

    time_shift_method = config.deid_config.time_shift.method

    if time_shift_method == "shift_by_days":
        issues.append(
            LintIssue(
                "cleared-009",
                "Using 'shift_by_days' may change day-of-week patterns (e.g., Monday → Friday). "
                "This can affect analyses that depend on weekday patterns (e.g., weekend vs weekday admissions).",
                severity="warning",
            )
        )
    elif time_shift_method == "shift_by_hours":
        issues.append(
            LintIssue(
                "cleared-009",
                "Using 'shift_by_hours' may change hour-of-day patterns (e.g., night shift → day shift). "
                "This can affect analyses that depend on time-of-day patterns (e.g., emergency vs scheduled procedures).",
                severity="warning",
            )
        )
    elif time_shift_method == "random_days":
        issues.append(
            LintIssue(
                "cleared-009",
                "Using 'random_days' may change day-of-week patterns. "
                "This can affect analyses that depend on weekday patterns.",
                severity="warning",
            )
        )
    elif time_shift_method == "random_hours":
        issues.append(
            LintIssue(
                "cleared-009",
                "Using 'random_hours' may change hour-of-day patterns. "
                "This can affect analyses that depend on time-of-day patterns.",
                severity="warning",
            )
        )

    return issues
