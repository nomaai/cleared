"""Core linting functionality for Cleared configuration files."""

from pathlib import Path

from cleared.config.structure import ClearedConfig

from cleared.lint.types import LintIssue
from cleared.lint.utils import (
    parse_ignore_comments,
    should_ignore_issue,
    map_issues_to_lines,
)
from cleared.lint.rules import (
    complexity,
    dependencies,
    format as format_rules,
    io,
    timeshift,
    transformers,
    uniqueness,
    validation,
)


def lint_cleared_config(config_path: Path, config: ClearedConfig) -> list[LintIssue]:
    """
    Apply all Cleared-specific linting rules to a configuration.

    This function orchestrates all individual linting rules and returns
    a combined list of issues found.

    Args:
        config_path: Path to the configuration file
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects from all rules

    """
    issues: list[LintIssue] = []

    # Validation rules
    issues.extend(validation.rule_required_keys(config))
    issues.extend(validation.rule_datetime_requires_timeshift(config))
    issues.extend(validation.rule_datetime_timeshift_defined(config))
    issues.extend(validation.rule_required_transformer_configs(config))

    # Dependency rules
    issues.extend(dependencies.rule_valid_table_dependencies(config))
    issues.extend(dependencies.rule_valid_transformer_dependencies(config))
    issues.extend(dependencies.rule_no_circular_dependencies(config))
    issues.extend(dependencies.rule_column_dropper_dependencies(config))

    # Uniqueness rules
    issues.extend(uniqueness.rule_unique_transformer_uids(config))
    issues.extend(uniqueness.rule_table_name_consistency(config))

    # Format rules
    issues.extend(format_rules.rule_uid_format(config))
    issues.extend(format_rules.rule_multiple_transformers_same_column(config))

    # Time shift rules
    issues.extend(timeshift.rule_timeshift_risk_warnings(config))
    issues.extend(timeshift.rule_timeshift_range_validation(config))

    # IO rules
    issues.extend(io.rule_io_configuration_validation(config))
    issues.extend(io.rule_output_paths_system_directories(config))
    issues.extend(io.rule_input_output_path_overlap(config))

    # Transformer rules
    issues.extend(transformers.rule_value_cast_appropriateness(config))
    issues.extend(transformers.rule_table_has_transformers(config))

    # Complexity rules
    issues.extend(complexity.rule_configuration_complexity(config_path, config))

    # Filter out issues that are ignored via comments
    # First, try to map issues to line numbers by searching the file
    ignore_map = parse_ignore_comments(config_path)

    # Map issues to line numbers if they don't have them
    issues_with_lines = map_issues_to_lines(issues, config_path, config)

    # Filter out ignored issues
    filtered_issues = [
        issue
        for issue in issues_with_lines
        if not should_ignore_issue(issue, ignore_map, issue.line)
    ]

    return filtered_issues
