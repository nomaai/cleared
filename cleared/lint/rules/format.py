"""Format and structure-related linting rules for Cleared configuration files."""

import re

from cleared.config.structure import ClearedConfig
from cleared.lint.types import LintIssue


def rule_uid_format(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-007: Check that transformer UIDs and table names have proper format.

    Valid format: lowercase alphanumeric characters and underscores, not starting/ending with underscore.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    # Pattern: lowercase alphanumeric and underscores, not starting/ending with underscore
    uid_pattern = re.compile(r"^[a-z0-9][a-z0-9_]*[a-z0-9]$|^[a-z0-9]$")

    # Check table names
    for table_name in config.tables.keys():
        if not uid_pattern.match(table_name):
            issues.append(
                LintIssue(
                    "cleared-007",
                    f"Table name '{table_name}' has invalid format. Expected: lowercase alphanumeric with underscores, not starting/ending with underscore",
                )
            )

    # Check transformer UIDs
    for table_name, table_config in config.tables.items():
        for transformer in table_config.transformers:
            if transformer.uid:
                if not uid_pattern.match(transformer.uid):
                    issues.append(
                        LintIssue(
                            "cleared-007",
                            f"Transformer UID '{transformer.uid}' in table '{table_name}' has invalid format. Expected: lowercase alphanumeric with underscores, not starting/ending with underscore",
                        )
                    )

    return issues


def rule_multiple_transformers_same_column(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-014: Check if multiple transformers without filters are trying to change the same column.

    This rule detects when multiple transformers in the same table are modifying the same column,
    which could lead to unexpected behavior or data loss.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    for table_name, table_config in config.tables.items():
        # Track columns being modified by transformers without filters
        columns_modified: dict[
            str, list[str]
        ] = {}  # column_name -> list of transformer UIDs

        for transformer in table_config.transformers:
            # Only check transformers without filters
            if transformer.filter is not None:
                continue

            transformer_uid = transformer.uid or "unnamed"
            configs = transformer.configs or {}

            column_name = None

            if transformer.method == "IDDeidentifier":
                # IDDeidentifier modifies the column specified in idconfig.name
                if "idconfig" in configs:
                    idconfig = configs["idconfig"]
                    if isinstance(idconfig, dict) and "name" in idconfig:
                        column_name = idconfig["name"]

            elif transformer.method == "DateTimeDeidentifier":
                # DateTimeDeidentifier modifies the column specified in datetime_column
                if "datetime_column" in configs:
                    datetime_col = configs["datetime_column"]
                    if isinstance(datetime_col, str):
                        column_name = datetime_col

            # ColumnDropper drops columns, so it doesn't "change" them in the same way
            # We don't include it in this check

            if column_name:
                if column_name not in columns_modified:
                    columns_modified[column_name] = []
                columns_modified[column_name].append(transformer_uid)

        # Report issues for columns modified by multiple transformers
        for column_name, transformer_uids in columns_modified.items():
            if len(transformer_uids) > 1:
                issues.append(
                    LintIssue(
                        "cleared-014",
                        f"Multiple transformers without filters are modifying column '{column_name}' "
                        f"in table '{table_name}': {', '.join(transformer_uids)}. "
                        f"This may cause unexpected behavior or data loss. "
                        f"Consider using filters to apply transformations conditionally or reordering transformers.",
                        severity="warning",
                    )
                )

    return issues
