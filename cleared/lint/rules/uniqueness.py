"""Uniqueness-related linting rules for Cleared configuration files."""

from cleared.config.structure import ClearedConfig
from cleared.lint.types import LintIssue


def rule_unique_transformer_uids(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-003: Check that transformer UIDs are unique across all tables.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    transformer_uids: dict[str, list[str]] = {}
    for table_name, table_config in config.tables.items():
        for transformer in table_config.transformers:
            if transformer.uid:
                if transformer.uid not in transformer_uids:
                    transformer_uids[transformer.uid] = []
                transformer_uids[transformer.uid].append(table_name)

    for uid, tables in transformer_uids.items():
        if len(tables) > 1:
            issues.append(
                LintIssue(
                    "cleared-003",
                    f"Transformer UID '{uid}' is used in multiple tables: {', '.join(tables)}",
                )
            )

    return issues


def rule_table_name_consistency(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-015: Check that table names are unique.

    Verifies that TableConfig.name values are unique across all tables.
    Dictionary keys are automatically unique in Python/YAML, but we check
    that the name field values are also unique to prevent confusion.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    # Track table names and which dictionary keys use them
    name_to_keys: dict[str, list[str]] = {}
    for table_key, table_config in config.tables.items():
        table_name = table_config.name
        if table_name not in name_to_keys:
            name_to_keys[table_name] = []
        name_to_keys[table_name].append(table_key)

    # Check for duplicate names
    for table_name, table_keys in name_to_keys.items():
        if len(table_keys) > 1:
            issues.append(
                LintIssue(
                    "cleared-015",
                    f"Table name '{table_name}' is used by multiple tables: {', '.join(table_keys)}. "
                    f"Table names must be unique.",
                )
            )

    return issues
