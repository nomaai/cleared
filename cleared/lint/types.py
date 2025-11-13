"""Type definitions for the linting package."""


class LintIssue:
    """Represents a linting issue."""

    def __init__(
        self, rule: str, message: str, line: int | None = None, severity: str = "error"
    ) -> None:
        """
        Initialize a linting issue.

        Args:
            rule: The rule ID (e.g., "cleared-011")
            message: The error or warning message
            line: Line number where the issue occurs (optional)
            severity: "error" or "warning" (default: "error")

        """
        self.rule = rule
        self.message = message
        self.line = line
        self.severity = severity  # 'error' or 'warning'

    def __str__(self) -> str:
        """Return string representation of the linting issue."""
        line_str = f":{self.line}" if self.line else ""
        return f"{self.severity.upper()}[{self.rule}]{line_str}: {self.message}"
