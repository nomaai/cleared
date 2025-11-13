"""Complexity-related linting rules for Cleared configuration files."""

from pathlib import Path

from cleared.config.structure import ClearedConfig
from cleared.lint.types import LintIssue


def rule_configuration_complexity(
    config_path: Path, config: ClearedConfig
) -> list[LintIssue]:
    """
    Rule cleared-020: Warn if configuration is overly complex.

    Configurations with more than 50 lines can be difficult to maintain and understand.
    This rule suggests breaking complex configurations into smaller files using
    Hydra's defaults functionality to import them.

    Args:
        config_path: Path to the configuration file
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    try:
        # Count non-empty lines in the configuration file
        with open(config_path, encoding="utf-8") as f:
            lines = f.readlines()

        # Count non-empty, non-comment lines
        non_empty_lines = 0
        for line in lines:
            stripped = line.strip()
            # Skip empty lines and comments
            if stripped and not stripped.startswith("#"):
                non_empty_lines += 1

        # Warn if configuration is overly complex (more than 50 non-empty lines)
        if non_empty_lines > 50:
            issues.append(
                LintIssue(
                    "cleared-020",
                    f"Configuration file has {non_empty_lines} non-empty lines (threshold: 50). "
                    f"Consider breaking this configuration into smaller, modular files using "
                    f"Hydra's defaults functionality. "
                    f"For example, you can split tables into separate files and import them using "
                    f"'defaults: [table1_config, table2_config, ...]' in the main configuration file.",
                    severity="warning",
                )
            )
    except OSError:
        # If we can't read the file, skip this check
        # (This shouldn't happen in normal operation, but handle gracefully)
        pass

    return issues
