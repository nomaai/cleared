"""Data models for verification results."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ColumnComparisonResult:
    """Result of comparing a single column."""

    column_name: str
    status: str  # "pass", "error", "warning"
    message: str
    original_length: int = 0
    reversed_length: int = 0
    mismatch_count: int = 0
    mismatch_percentage: float = 0.0
    sample_mismatch_indices: list[int] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate column comparison result data."""
        _validate_column_comparison_result(self)


@dataclass
class TableVerificationResult:
    """Result of verifying a single table."""

    table_name: str
    status: str  # "pass", "error", "warning"
    total_columns: int
    passed_columns: int
    error_columns: int
    warning_columns: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    column_results: list[ColumnComparisonResult] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate table verification result data."""
        _validate_table_verification_result(self)


@dataclass
class VerificationOverview:
    """Overview statistics for the verification."""

    total_tables: int
    passed_tables: int
    failed_tables: int
    warning_tables: int
    total_errors: int
    total_warnings: int
    total_columns_checked: int
    total_columns_passed: int
    total_columns_errored: int
    total_columns_warned: int

    def __post_init__(self) -> None:
        """Validate verification overview data."""
        _validate_verification_overview(self)


@dataclass
class VerificationResult:
    """Complete verification result."""

    overview: VerificationOverview
    tables: list[TableVerificationResult]
    config_path: str
    reverse_data_path: str

    def __post_init__(self) -> None:
        """Validate verification result data."""
        _validate_verification_result(self)


# Validation helper functions


def _validate_column_comparison_result(result: ColumnComparisonResult) -> None:
    """Validate column comparison result data."""
    # Validate status
    valid_statuses = {"pass", "error", "warning"}
    if result.status not in valid_statuses:
        raise ValueError(
            f"Invalid status '{result.status}'. Must be one of {valid_statuses}"
        )

    # Validate column_name is not empty
    if not result.column_name or not isinstance(result.column_name, str):
        raise ValueError("column_name must be a non-empty string")

    # Validate message is not empty
    if not result.message or not isinstance(result.message, str):
        raise ValueError("message must be a non-empty string")

    # Validate status-specific constraints
    if result.status == "pass":
        if result.mismatch_count != 0:
            raise ValueError(
                f"Pass status requires mismatch_count to be 0, got {result.mismatch_count}"
            )
        if result.mismatch_percentage != 0.0:
            raise ValueError(
                f"Pass status requires mismatch_percentage to be 0.0, got {result.mismatch_percentage}"
            )
        if result.sample_mismatch_indices:
            raise ValueError(
                f"Pass status should have no mismatch indices, got {len(result.sample_mismatch_indices)}"
            )

    # Validate mismatch_count consistency
    if result.original_length > 0 and result.mismatch_count > result.original_length:
        raise ValueError(
            f"mismatch_count ({result.mismatch_count}) cannot exceed original_length ({result.original_length})"
        )

    # Validate mismatch_percentage consistency
    if result.mismatch_percentage < 0.0 or result.mismatch_percentage > 100.0:
        raise ValueError(
            f"mismatch_percentage must be between 0.0 and 100.0, got {result.mismatch_percentage}"
        )
    # If both mismatch_count and mismatch_percentage are set, they should be consistent
    if result.mismatch_count > 0 and result.original_length > 0:
        expected_percentage = (result.mismatch_count / result.original_length) * 100.0
        if (
            abs(result.mismatch_percentage - expected_percentage) > 0.01
        ):  # Allow small floating point errors
            raise ValueError(
                f"mismatch_percentage ({result.mismatch_percentage}) does not match "
                f"calculated value ({expected_percentage:.2f}) from mismatch_count and original_length"
            )

    # Validate sample_mismatch_indices
    if result.sample_mismatch_indices:
        if result.mismatch_count == 0:
            raise ValueError(
                "sample_mismatch_indices should be empty when mismatch_count is 0"
            )
        if len(result.sample_mismatch_indices) > result.mismatch_count:
            raise ValueError(
                f"sample_mismatch_indices length ({len(result.sample_mismatch_indices)}) "
                f"cannot exceed mismatch_count ({result.mismatch_count})"
            )
        if result.original_length > 0:
            invalid_indices = [
                idx
                for idx in result.sample_mismatch_indices
                if idx >= result.original_length
            ]
            if invalid_indices:
                raise ValueError(
                    f"sample_mismatch_indices contains invalid indices {invalid_indices} "
                    f"that exceed original_length ({result.original_length})"
                )


def _validate_table_verification_result(result: TableVerificationResult) -> None:
    """Validate table verification result data."""
    # Validate status
    valid_statuses = {"pass", "error", "warning"}
    if result.status not in valid_statuses:
        raise ValueError(
            f"Invalid status '{result.status}'. Must be one of {valid_statuses}"
        )

    # Validate table_name is not empty
    if not result.table_name or not isinstance(result.table_name, str):
        raise ValueError("table_name must be a non-empty string")

    # Validate that column counts add up
    calculated_total = (
        result.passed_columns + result.error_columns + result.warning_columns
    )
    if calculated_total != result.total_columns:
        raise ValueError(
            f"Column counts do not add up: passed ({result.passed_columns}) + "
            f"error ({result.error_columns}) + warning ({result.warning_columns}) = "
            f"{calculated_total}, but total_columns is {result.total_columns}"
        )

    # Validate status consistency
    if result.status == "pass" and (result.error_columns > 0 or len(result.errors) > 0):
        raise ValueError(
            f"Pass status requires no errors, but got {result.error_columns} error columns "
            f"and {len(result.errors)} error messages"
        )
    if (
        result.status == "error"
        and result.error_columns == 0
        and len(result.errors) == 0
    ):
        raise ValueError(
            "Error status requires at least one error column or error message"
        )

    # Validate column_results consistency
    if len(result.column_results) != result.total_columns:
        raise ValueError(
            f"column_results length ({len(result.column_results)}) does not match "
            f"total_columns ({result.total_columns})"
        )

    # Validate column_results counts match
    actual_passed = sum(1 for r in result.column_results if r.status == "pass")
    actual_error = sum(1 for r in result.column_results if r.status == "error")
    actual_warning = sum(1 for r in result.column_results if r.status == "warning")

    if actual_passed != result.passed_columns:
        raise ValueError(
            f"passed_columns ({result.passed_columns}) does not match actual count "
            f"from column_results ({actual_passed})"
        )
    if actual_error != result.error_columns:
        raise ValueError(
            f"error_columns ({result.error_columns}) does not match actual count "
            f"from column_results ({actual_error})"
        )
    if actual_warning != result.warning_columns:
        raise ValueError(
            f"warning_columns ({result.warning_columns}) does not match actual count "
            f"from column_results ({actual_warning})"
        )

    # Validate errors and warnings lists match column_results
    error_messages_from_results = [
        r.message for r in result.column_results if r.status == "error"
    ]
    warning_messages_from_results = [
        r.message for r in result.column_results if r.status == "warning"
    ]

    if len(error_messages_from_results) != len(result.errors):
        raise ValueError(
            f"errors list length ({len(result.errors)}) does not match error column_results "
            f"count ({len(error_messages_from_results)})"
        )

    if len(warning_messages_from_results) != len(result.warnings):
        raise ValueError(
            f"warnings list length ({len(result.warnings)}) does not match warning column_results "
            f"count ({len(warning_messages_from_results)})"
        )


def _validate_verification_overview(overview: VerificationOverview) -> None:
    """Validate verification overview data."""
    # Validate table counts add up
    calculated_total_tables = (
        overview.passed_tables + overview.failed_tables + overview.warning_tables
    )
    if calculated_total_tables != overview.total_tables:
        raise ValueError(
            f"Table counts do not add up: passed ({overview.passed_tables}) + "
            f"failed ({overview.failed_tables}) + warning ({overview.warning_tables}) = "
            f"{calculated_total_tables}, but total_tables is {overview.total_tables}"
        )

    # Validate column counts add up
    calculated_total_columns = (
        overview.total_columns_passed
        + overview.total_columns_errored
        + overview.total_columns_warned
    )
    if calculated_total_columns != overview.total_columns_checked:
        raise ValueError(
            f"Column counts do not add up: passed ({overview.total_columns_passed}) + "
            f"errored ({overview.total_columns_errored}) + warned ({overview.total_columns_warned}) = "
            f"{calculated_total_columns}, but total_columns_checked is {overview.total_columns_checked}"
        )

    # Validate that counts don't exceed totals
    if overview.passed_tables > overview.total_tables:
        raise ValueError(
            f"passed_tables ({overview.passed_tables}) cannot exceed total_tables ({overview.total_tables})"
        )
    if overview.failed_tables > overview.total_tables:
        raise ValueError(
            f"failed_tables ({overview.failed_tables}) cannot exceed total_tables ({overview.total_tables})"
        )
    if overview.warning_tables > overview.total_tables:
        raise ValueError(
            f"warning_tables ({overview.warning_tables}) cannot exceed total_tables ({overview.total_tables})"
        )

    if overview.total_columns_passed > overview.total_columns_checked:
        raise ValueError(
            f"total_columns_passed ({overview.total_columns_passed}) cannot exceed "
            f"total_columns_checked ({overview.total_columns_checked})"
        )
    if overview.total_columns_errored > overview.total_columns_checked:
        raise ValueError(
            f"total_columns_errored ({overview.total_columns_errored}) cannot exceed "
            f"total_columns_checked ({overview.total_columns_checked})"
        )
    if overview.total_columns_warned > overview.total_columns_checked:
        raise ValueError(
            f"total_columns_warned ({overview.total_columns_warned}) cannot exceed "
            f"total_columns_checked ({overview.total_columns_checked})"
        )


def _validate_verification_result(result: VerificationResult) -> None:
    """Validate verification result data."""
    # Validate config_path is not empty
    if not result.config_path or not isinstance(result.config_path, str):
        raise ValueError("config_path must be a non-empty string")

    # Validate reverse_data_path is not empty
    if not result.reverse_data_path or not isinstance(result.reverse_data_path, str):
        raise ValueError("reverse_data_path must be a non-empty string")

    # Validate overview matches tables
    if len(result.tables) != result.overview.total_tables:
        raise ValueError(
            f"tables length ({len(result.tables)}) does not match "
            f"overview.total_tables ({result.overview.total_tables})"
        )

    # Validate table status counts match overview
    actual_passed = sum(1 for t in result.tables if t.status == "pass")
    actual_failed = sum(1 for t in result.tables if t.status == "error")
    actual_warning = sum(1 for t in result.tables if t.status == "warning")

    if actual_passed != result.overview.passed_tables:
        raise ValueError(
            f"overview.passed_tables ({result.overview.passed_tables}) does not match "
            f"actual count from tables ({actual_passed})"
        )
    if actual_failed != result.overview.failed_tables:
        raise ValueError(
            f"overview.failed_tables ({result.overview.failed_tables}) does not match "
            f"actual count from tables ({actual_failed})"
        )
    if actual_warning != result.overview.warning_tables:
        raise ValueError(
            f"overview.warning_tables ({result.overview.warning_tables}) does not match "
            f"actual count from tables ({actual_warning})"
        )

    # Validate error and warning counts match
    actual_total_errors = sum(len(t.errors) for t in result.tables)
    actual_total_warnings = sum(len(t.warnings) for t in result.tables)

    if actual_total_errors != result.overview.total_errors:
        raise ValueError(
            f"overview.total_errors ({result.overview.total_errors}) does not match "
            f"actual count from tables ({actual_total_errors})"
        )
    if actual_total_warnings != result.overview.total_warnings:
        raise ValueError(
            f"overview.total_warnings ({result.overview.total_warnings}) does not match "
            f"actual count from tables ({actual_total_warnings})"
        )

    # Validate column counts match
    actual_columns_checked = sum(t.total_columns for t in result.tables)
    actual_columns_passed = sum(t.passed_columns for t in result.tables)
    actual_columns_errored = sum(t.error_columns for t in result.tables)
    actual_columns_warned = sum(t.warning_columns for t in result.tables)

    if actual_columns_checked != result.overview.total_columns_checked:
        raise ValueError(
            f"overview.total_columns_checked ({result.overview.total_columns_checked}) does not match "
            f"actual count from tables ({actual_columns_checked})"
        )
    if actual_columns_passed != result.overview.total_columns_passed:
        raise ValueError(
            f"overview.total_columns_passed ({result.overview.total_columns_passed}) does not match "
            f"actual count from tables ({actual_columns_passed})"
        )
    if actual_columns_errored != result.overview.total_columns_errored:
        raise ValueError(
            f"overview.total_columns_errored ({result.overview.total_columns_errored}) does not match "
            f"actual count from tables ({actual_columns_errored})"
        )
    if actual_columns_warned != result.overview.total_columns_warned:
        raise ValueError(
            f"overview.total_columns_warned ({result.overview.total_columns_warned}) does not match "
            f"actual count from tables ({actual_columns_warned})"
        )

    # Validate table names are unique
    table_names = [t.table_name for t in result.tables]
    if len(table_names) != len(set(table_names)):
        duplicates = [name for name in table_names if table_names.count(name) > 1]
        raise ValueError(f"Duplicate table names found: {set(duplicates)}")
