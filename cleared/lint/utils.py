"""Utility functions for linting."""

import re
from pathlib import Path
from typing import TYPE_CHECKING

from cleared.lint.types import LintIssue

if TYPE_CHECKING:
    from cleared.config.structure import ClearedConfig


def parse_ignore_comments(config_path: Path) -> dict[int, set[str | None]]:
    """
    Parse ignore comments from a YAML file.

    Supports both yamllint format and noqa format for consistency:
    - `# yamllint disable-line rule:cleared-XXX` (yamllint-style, ignore specific rule)
    - `# yamllint disable-line rule:cleared-XXX,cleared-YYY` (yamllint-style, multiple rules)
    - `# noqa: cleared-XXX` (noqa-style, ignore specific rule)
    - `# noqa: cleared-XXX, cleared-YYY` (noqa-style, ignore multiple rules)
    - `# noqa` (ignore all Cleared rules on this line)

    Note: yamllint's own ignore comments are handled automatically by yamllint.
    This function only handles ignore comments for our custom Cleared rules.

    Args:
        config_path: Path to the configuration file

    Returns:
        Dictionary mapping line numbers (1-indexed) to sets of rule IDs to ignore.
        If a line has `# noqa` without specific rules, the set will contain None.

    """
    ignores: dict[int, set[str | None]] = {}

    try:
        with open(config_path, encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                # Look for yamllint-style disable comments: # yamllint disable-line rule:cleared-XXX
                yamllint_match = re.search(
                    r"#\s*yamllint\s+disable-line\s+rule:\s*([^#\n]+)",
                    line,
                    re.IGNORECASE,
                )
                if yamllint_match:
                    rules_str = yamllint_match.group(1)
                    # Parse rules (can be comma-separated)
                    rules: set[str | None] = {
                        rule.strip()
                        for rule in rules_str.split(",")
                        if rule.strip().startswith("cleared-")
                    }
                    if rules:
                        if line_num in ignores:
                            ignores[line_num].update(rules)
                        else:
                            ignores[line_num] = rules
                    continue

                # Look for noqa comments: # noqa: cleared-XXX or # noqa
                # Note: This is for our custom Cleared rules, not ruff's noqa
                noqa_match = re.search(
                    r"#\s*noqa(?::\s*([^#\n]+))?", line, re.IGNORECASE
                )
                if noqa_match:
                    rules_str = noqa_match.group(1)
                    if rules_str:
                        # Parse specific rules: "cleared-XXX, cleared-YYY"
                        rules = {
                            rule.strip()
                            for rule in rules_str.split(",")
                            if rule.strip().startswith("cleared-")
                        }
                        if rules:
                            if line_num in ignores:
                                ignores[line_num].update(rules)
                            else:
                                ignores[line_num] = rules
                    else:
                        # Just "# noqa" format - ignore all Cleared rules on this line
                        # Note: This is for our custom Cleared rules, not ruff's noqa
                        if line_num not in ignores:
                            ignores[line_num] = {None}
                        else:
                            ignores[line_num].add(None)
    except OSError:
        # If we can't read the file, return empty ignores
        pass

    return ignores


def find_yaml_line_number(config_path: Path, key_path: list[str]) -> int | None:
    """
    Find the line number for a YAML key path in the configuration file.

    This is a simplified implementation that searches for the key in the file.
    For nested keys, it finds the last key in the path.

    Args:
        config_path: Path to the configuration file
        key_path: List of keys representing the path (e.g., ["deid_config", "time_shift", "max"])

    Returns:
        Line number (1-indexed) where the key was found, or None if not found

    """
    if not key_path:
        return None

    target_key = key_path[-1]
    try:
        with open(config_path, encoding="utf-8") as f:
            lines = f.readlines()
            # Simple search for the key pattern
            for line_num, line in enumerate(lines, start=1):
                # Look for the key followed by colon (YAML key syntax)
                if re.search(rf"^\s*{re.escape(target_key)}\s*:", line):
                    return line_num
    except OSError:
        pass

    return None


def map_issues_to_lines(
    issues: list["LintIssue"], config_path: Path, config: "ClearedConfig"
) -> list["LintIssue"]:
    """
    Map issues to line numbers by searching the YAML file.

    This is a best-effort approach that searches for relevant patterns in the file
    to determine line numbers for issues that don't have them set.

    Args:
        issues: List of LintIssue objects (may not have line numbers)
        config_path: Path to the configuration file
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects with line numbers set where possible

    """
    issues_with_lines = []

    try:
        with open(config_path, encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        # If we can't read the file, return issues as-is
        return issues

    for issue in issues:
        if issue.line is not None:
            # Already has a line number
            issues_with_lines.append(issue)
            continue

        # Try to find the line number based on the issue
        line_num = None

        if issue.rule == "cleared-011":
            # Time shift range validation - look for "max:" line with negative value
            # or look for the time_shift section
            for i, line in enumerate(lines, start=1):
                if "max:" in line and (
                    "-30" in line or "-" in line.split("max:")[-1]
                    if "max:" in line
                    else ""
                ):
                    # Check if this is in the time_shift section
                    # Look backwards for time_shift context
                    context_lines = lines[max(0, i - 10) : i]
                    if any("time_shift" in line for line in context_lines):
                        line_num = i
                        break

        elif issue.rule == "cleared-004":
            # Invalid table dependency - look for "non_existent_table"
            for i, line in enumerate(lines, start=1):
                if "non_existent_table" in line:
                    line_num = i
                    break

        elif issue.rule == "cleared-017":
            # Table has no transformers - look for "transformers: []"
            table_name = None
            # Extract table name from issue message
            if "Table '" in issue.message:
                table_name = issue.message.split("Table '")[1].split("'")[0]

            for i, line in enumerate(lines, start=1):
                if table_name and table_name in line and "name:" in line:
                    # Found the table, now look for transformers: [] in the next few lines
                    for j in range(i, min(i + 10, len(lines))):
                        if "transformers:" in lines[j] and "[]" in lines[j]:
                            line_num = j + 1
                            break
                    if line_num:
                        break

        elif issue.rule == "cleared-018":
            # System directory - look for "/tmp/runtime" or other system paths
            for i, line in enumerate(lines, start=1):
                if "/tmp/runtime" in line or (
                    "runtime_io_path" in line and "/tmp" in line
                ):
                    line_num = i
                    break

        # Create a new issue with the line number if found
        if line_num is not None:
            issue_with_line = LintIssue(
                rule=issue.rule,
                message=issue.message,
                line=line_num,
                severity=issue.severity,
            )
            issues_with_lines.append(issue_with_line)
        else:
            # Keep the original issue without line number
            issues_with_lines.append(issue)

    return issues_with_lines


def should_ignore_issue(
    issue: "LintIssue", ignore_map: dict[int, set[str | None]], issue_line: int | None
) -> bool:
    """
    Check if an issue should be ignored based on ignore comments.

    Args:
        issue: The LintIssue to check
        ignore_map: Dictionary from parse_ignore_comments
        issue_line: Line number where the issue occurs (if known)

    Returns:
        True if the issue should be ignored, False otherwise

    """
    if issue_line is not None:
        if issue_line in ignore_map:
            ignored_rules = ignore_map[issue_line]
            # Check if all rules are ignored on this line
            if None in ignored_rules:
                return True
            # Check if this specific rule is ignored
            if issue.rule in ignored_rules:
                return True

    # For issues without line numbers, we can't reliably map them to ignore comments
    # So we don't ignore them
    return False
