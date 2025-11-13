"""Linting package for Cleared configuration files."""

from cleared.lint.core import lint_cleared_config
from cleared.lint.types import LintIssue

__all__ = ["LintIssue", "lint_cleared_config"]
