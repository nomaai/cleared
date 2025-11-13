"""Transformer-related linting rules for Cleared configuration files."""

from cleared.config.structure import ClearedConfig
from cleared.lint.types import LintIssue


def rule_value_cast_appropriateness(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-016: Validate that value_cast is used appropriately for each transformer type.

    - Checks that value_cast is only used with transformers that support it (FilterableTransformer subclasses)
    - Warns if value_cast seems inappropriate for the transformer type:
      * IDDeidentifier: typically uses "integer" or "string" (warn if "datetime" is used)
      * DateTimeDeidentifier: typically uses "datetime" (warn if "integer" or "float" is used)

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    # Transformers that support value_cast (inherit from FilterableTransformer)
    transformers_supporting_value_cast = {"IDDeidentifier", "DateTimeDeidentifier"}

    for table_name, table_config in config.tables.items():
        for transformer in table_config.transformers:
            if transformer.value_cast is None:
                continue

            transformer_method = transformer.method
            value_cast = transformer.value_cast

            # Check if transformer supports value_cast
            if transformer_method not in transformers_supporting_value_cast:
                issues.append(
                    LintIssue(
                        "cleared-016",
                        f"Transformer '{transformer_method}' in table '{table_name}' does not support value_cast, "
                        f"but value_cast='{value_cast}' was provided. "
                        f"Only transformers that inherit from FilterableTransformer support value_cast "
                        f"(currently: IDDeidentifier, DateTimeDeidentifier).",
                    )
                )
                continue

            # Check for potentially inappropriate value_cast values
            if transformer_method == "IDDeidentifier":
                if value_cast == "datetime":
                    issues.append(
                        LintIssue(
                            "cleared-016",
                            f"IDDeidentifier in table '{table_name}' uses value_cast='datetime', "
                            f"which is unusual for ID columns. IDDeidentifier typically uses "
                            f"'integer' or 'string' for value_cast. "
                            f"Consider using 'integer' if IDs are numeric or 'string' if they are text.",
                            severity="warning",
                        )
                    )
            elif transformer_method == "DateTimeDeidentifier":
                if value_cast in ("integer", "float"):
                    issues.append(
                        LintIssue(
                            "cleared-016",
                            f"DateTimeDeidentifier in table '{table_name}' uses value_cast='{value_cast}', "
                            f"which is unusual for datetime columns. DateTimeDeidentifier typically uses "
                            f"'datetime' for value_cast. "
                            f"Consider using 'datetime' to properly parse datetime strings.",
                            severity="warning",
                        )
                    )

    return issues


def rule_table_has_transformers(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-017: Warn if a table has no transformers.

    Tables without transformers will not perform any de-identification,
    which may be unintentional. This rule warns about such tables.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    for table_name, table_config in config.tables.items():
        if not table_config.transformers or len(table_config.transformers) == 0:
            issues.append(
                LintIssue(
                    "cleared-017",
                    f"Table '{table_name}' has no transformers. "
                    f"No de-identification will be performed on this table. "
                    f"If this is intentional, you may ignore this warning.",
                    severity="warning",
                )
            )

    return issues
