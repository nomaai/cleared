"""Validation rules for Cleared configuration files."""

from cleared.config.structure import ClearedConfig
from cleared.lint.types import LintIssue


def rule_required_keys(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-001: Check that required top-level keys exist.

    This rule checks the merged ClearedConfig object, not the raw YAML file,
    to properly handle Hydra-style imports where keys may be in sub-files.

    Args:
        config: Loaded ClearedConfig object (already merged if using Hydra)

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    # Check required attributes in the ClearedConfig object
    # Note: We check for None/empty rather than truthiness to handle empty strings
    if config.name is None or config.name == "":
        issues.append(LintIssue("cleared-001", "Missing required key: name"))

    if config.deid_config is None:
        issues.append(LintIssue("cleared-001", "Missing required key: deid_config"))

    if config.io is None:
        issues.append(LintIssue("cleared-001", "Missing required key: io"))

    if config.tables is None or len(config.tables) == 0:
        issues.append(LintIssue("cleared-001", "Missing required key: tables"))

    return issues


def rule_datetime_requires_timeshift(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-002: Check that deid_config has time_shift if DateTimeDeidentifier is used.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    has_datetime_transformer = False
    for table_config in config.tables.values():
        for transformer in table_config.transformers:
            if transformer.method == "DateTimeDeidentifier":
                has_datetime_transformer = True
                break
        if has_datetime_transformer:
            break

    if has_datetime_transformer and not config.deid_config.time_shift:
        issues.append(
            LintIssue(
                "cleared-002",
                "DateTimeDeidentifier requires time_shift in global deid_config",
            )
        )

    return issues


def rule_datetime_timeshift_defined(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-008: Check that if DateTimeDeidentifier is used, deid_config and time_shift are properly defined.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    has_datetime_transformer = False
    for table_config in config.tables.values():
        for transformer in table_config.transformers:
            if transformer.method == "DateTimeDeidentifier":
                has_datetime_transformer = True
                break
        if has_datetime_transformer:
            break

    if has_datetime_transformer:
        if not config.deid_config:
            issues.append(
                LintIssue(
                    "cleared-008",
                    "DateTimeDeidentifier is used but deid_config is not defined",
                )
            )
        elif not config.deid_config.time_shift:
            issues.append(
                LintIssue(
                    "cleared-008",
                    "DateTimeDeidentifier is used but time_shift is not defined in deid_config",
                )
            )
        elif not config.deid_config.time_shift.method:
            issues.append(
                LintIssue(
                    "cleared-008",
                    "DateTimeDeidentifier is used but time_shift.method is not defined",
                )
            )

    return issues


def rule_required_transformer_configs(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-012: Validate that transformers have required configs.

    - IDDeidentifier: requires idconfig with name and uid
    - DateTimeDeidentifier: requires idconfig and datetime_column
    - ColumnDropper: requires idconfig with name

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    for table_name, table_config in config.tables.items():
        for transformer in table_config.transformers:
            transformer_uid = transformer.uid or "unnamed"
            configs = transformer.configs or {}

            if transformer.method == "IDDeidentifier":
                # IDDeidentifier requires idconfig with name and uid
                if "idconfig" not in configs:
                    issues.append(
                        LintIssue(
                            "cleared-012",
                            f"Transformer '{transformer_uid}' (IDDeidentifier) in table '{table_name}' "
                            f"is missing required config 'idconfig'.",
                        )
                    )
                else:
                    idconfig = configs["idconfig"]
                    if isinstance(idconfig, dict):
                        if "name" not in idconfig:
                            issues.append(
                                LintIssue(
                                    "cleared-012",
                                    f"Transformer '{transformer_uid}' (IDDeidentifier) in table '{table_name}' "
                                    f"is missing required 'idconfig.name'.",
                                )
                            )
                        if "uid" not in idconfig:
                            issues.append(
                                LintIssue(
                                    "cleared-012",
                                    f"Transformer '{transformer_uid}' (IDDeidentifier) in table '{table_name}' "
                                    f"is missing required 'idconfig.uid'.",
                                )
                            )

            elif transformer.method == "DateTimeDeidentifier":
                # DateTimeDeidentifier requires idconfig and datetime_column
                if "idconfig" not in configs:
                    issues.append(
                        LintIssue(
                            "cleared-012",
                            f"Transformer '{transformer_uid}' (DateTimeDeidentifier) in table '{table_name}' "
                            f"is missing required config 'idconfig'.",
                        )
                    )
                if "datetime_column" not in configs:
                    issues.append(
                        LintIssue(
                            "cleared-012",
                            f"Transformer '{transformer_uid}' (DateTimeDeidentifier) in table '{table_name}' "
                            f"is missing required config 'datetime_column'.",
                        )
                    )

            elif transformer.method == "ColumnDropper":
                # ColumnDropper requires idconfig with name
                if "idconfig" not in configs:
                    issues.append(
                        LintIssue(
                            "cleared-012",
                            f"Transformer '{transformer_uid}' (ColumnDropper) in table '{table_name}' "
                            f"is missing required config 'idconfig'.",
                        )
                    )
                else:
                    idconfig = configs["idconfig"]
                    if isinstance(idconfig, dict):
                        if "name" not in idconfig:
                            issues.append(
                                LintIssue(
                                    "cleared-012",
                                    f"Transformer '{transformer_uid}' (ColumnDropper) in table '{table_name}' "
                                    f"is missing required 'idconfig.name'.",
                                )
                            )

    return issues
