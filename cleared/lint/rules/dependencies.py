"""Dependency-related linting rules for Cleared configuration files."""

from typing import Any

from cleared.config.structure import ClearedConfig
from cleared.lint.types import LintIssue


def rule_valid_table_dependencies(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-004: Check that table dependencies reference existing tables.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    table_names = set(config.tables.keys())
    for table_name, table_config in config.tables.items():
        for dep in table_config.depends_on:
            if dep not in table_names:
                issues.append(
                    LintIssue(
                        "cleared-004",
                        f"Table '{table_name}' depends on non-existent table '{dep}'",
                    )
                )

    return issues


def rule_valid_transformer_dependencies(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-005: Check that transformer dependencies reference existing transformers in the same table.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    for table_name, table_config in config.tables.items():
        transformer_uids_in_table = {t.uid for t in table_config.transformers if t.uid}
        for transformer in table_config.transformers:
            for dep_uid in transformer.depends_on:
                if dep_uid not in transformer_uids_in_table:
                    issues.append(
                        LintIssue(
                            "cleared-005",
                            f"Transformer '{transformer.uid}' in table '{table_name}' depends on non-existent transformer '{dep_uid}'",
                        )
                    )

    return issues


def rule_no_circular_dependencies(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-006: Check for circular dependencies in tables and transformers.

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    # Check for circular table dependencies
    def find_table_cycle_path(
        start: str, path: list[str], visited: set[str]
    ) -> list[str] | None:
        """Find a cycle path starting from start table."""
        if start in path:
            # Found a cycle - return the cycle portion
            cycle_start = path.index(start)
            return [*path[cycle_start:], start]

        if start in visited or start not in config.tables:
            return None

        visited.add(start)
        path.append(start)

        for dep in config.tables[start].depends_on:
            cycle = find_table_cycle_path(dep, path.copy(), visited.copy())
            if cycle:
                return cycle

        return None

    detected_table_cycles: set[tuple[str, ...]] = set()
    for table_name in config.tables.keys():
        cycle = find_table_cycle_path(table_name, [], set())
        if cycle:
            # Normalize cycle (start from smallest table name to avoid duplicates)
            cycle_tuple = tuple(sorted(set(cycle)))
            if cycle_tuple not in detected_table_cycles:
                detected_table_cycles.add(cycle_tuple)
                issues.append(
                    LintIssue(
                        "cleared-006",
                        f"Circular table dependency detected: {' → '.join(cycle)}",
                    )
                )

    # Check for circular transformer dependencies within each table
    for table_name, table_config in config.tables.items():
        transformer_map = {t.uid: t for t in table_config.transformers if t.uid}

        def find_transformer_cycle_path(
            start: str, path: list[str], visited: set[str], t_map: dict[str, Any]
        ) -> list[str] | None:
            """Find a cycle path starting from start transformer."""
            if start not in t_map:
                return None

            if start in path:
                # Found a cycle - return the cycle portion
                cycle_start = path.index(start)
                return [*path[cycle_start:], start]

            if start in visited:
                return None

            visited.add(start)
            path.append(start)

            transformer = t_map[start]
            for dep_uid in transformer.depends_on:
                cycle = find_transformer_cycle_path(
                    dep_uid, path.copy(), visited.copy(), t_map
                )
                if cycle:
                    return cycle

            return None

        detected_transformer_cycles: set[tuple[str, ...]] = set()
        for transformer in table_config.transformers:
            if transformer.uid:
                cycle = find_transformer_cycle_path(
                    transformer.uid, [], set(), transformer_map
                )
                if cycle:
                    # Normalize cycle to avoid duplicates
                    cycle_tuple = tuple(sorted(set(cycle)))
                    if cycle_tuple not in detected_transformer_cycles:
                        detected_transformer_cycles.add(cycle_tuple)
                        issues.append(
                            LintIssue(
                                "cleared-006",
                                f"Circular transformer dependency in table '{table_name}': {' → '.join(cycle)}",
                            )
                        )

    return issues


def rule_column_dropper_dependencies(config: ClearedConfig) -> list[LintIssue]:
    """
    Rule cleared-010: Check if ColumnDropper removes columns that other transformers depend on.

    This rule checks if a ColumnDropper transformer removes a column that is used by
    other transformers either as:
    - A reference ID column (idconfig.name)
    - A datetime column (datetime_column for DateTimeDeidentifier)

    Args:
        config: Loaded ClearedConfig object

    Returns:
        List of LintIssue objects

    """
    issues: list[LintIssue] = []

    for table_name, table_config in config.tables.items():
        # Build execution order based on dependencies
        transformer_map = {t.uid: t for t in table_config.transformers if t.uid}
        execution_order = _build_execution_order(
            table_config.transformers, transformer_map
        )

        # Track columns dropped by ColumnDroppers and when they're dropped
        dropped_columns: dict[str, int] = {}  # column_name -> execution_index

        # First pass: identify all columns that will be dropped and when
        for idx, transformer in enumerate(execution_order):
            if transformer.method == "ColumnDropper":
                # Extract the column name being dropped
                if transformer.configs and "idconfig" in transformer.configs:
                    idconfig_data = transformer.configs["idconfig"]
                    if isinstance(idconfig_data, dict) and "name" in idconfig_data:
                        dropped_column = idconfig_data["name"]
                        dropped_columns[dropped_column] = idx

        # Second pass: check if any transformer that executes after a ColumnDropper
        # uses a column that was dropped
        for idx, transformer in enumerate(execution_order):
            # Check if this transformer uses any dropped columns
            columns_used: list[str] = []

            # Get reference ID column (idconfig.name)
            if transformer.configs and "idconfig" in transformer.configs:
                idconfig_data = transformer.configs["idconfig"]
                if isinstance(idconfig_data, dict) and "name" in idconfig_data:
                    columns_used.append(idconfig_data["name"])

            # Get datetime column for DateTimeDeidentifier
            if transformer.method == "DateTimeDeidentifier":
                if transformer.configs and "datetime_column" in transformer.configs:
                    datetime_col = transformer.configs["datetime_column"]
                    if isinstance(datetime_col, str):
                        columns_used.append(datetime_col)

            # Check if any used column was dropped earlier
            for col in columns_used:
                if col in dropped_columns:
                    drop_idx = dropped_columns[col]
                    if drop_idx < idx:
                        # Find the ColumnDropper that drops this column
                        dropper = execution_order[drop_idx]
                        issues.append(
                            LintIssue(
                                "cleared-010",
                                f"Transformer '{transformer.uid}' in table '{table_name}' uses column '{col}' "
                                f"which is dropped by ColumnDropper '{dropper.uid}' earlier in the pipeline. "
                                f"Consider reordering transformers or removing the dependency on '{col}'.",
                            )
                        )

    return issues


def _build_execution_order(
    transformers: list, transformer_map: dict[str, Any]
) -> list[Any]:
    """
    Build execution order for transformers based on dependencies.

    Uses topological sort to determine the order in which transformers should execute.

    Args:
        transformers: List of transformer configs
        transformer_map: Dictionary mapping UID to transformer config

    Returns:
        List of transformers in execution order

    """
    # Build dependency graph
    graph: dict[str, list[str]] = {}
    in_degree: dict[str, int] = {}
    uid_to_transformer: dict[str, Any] = {}

    for transformer in transformers:
        if transformer.uid:
            uid_to_transformer[transformer.uid] = transformer
            graph[transformer.uid] = transformer.depends_on.copy()
            in_degree[transformer.uid] = len(transformer.depends_on)

    # Topological sort
    queue: list[str] = [uid for uid, degree in in_degree.items() if degree == 0]
    execution_order: list[Any] = []

    while queue:
        current_uid = queue.pop(0)
        execution_order.append(uid_to_transformer[current_uid])

        # Reduce in-degree for dependents
        for uid, deps in graph.items():
            if current_uid in deps:
                in_degree[uid] -= 1
                if in_degree[uid] == 0:
                    queue.append(uid)

    # Handle any remaining transformers (shouldn't happen if no cycles, but handle gracefully)
    for _uid, transformer in uid_to_transformer.items():
        if transformer not in execution_order:
            execution_order.append(transformer)

    return execution_order
