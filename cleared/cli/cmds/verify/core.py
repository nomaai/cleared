"""CLI command mode and core verification logic."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import typer

from cleared.cli.utils import load_config_from_file
from cleared.cli.cmds.verify.model import (
    ColumnComparisonResult,
    TableVerificationResult,
    VerificationOverview,
    VerificationResult,
)
from cleared.cli.cmds.verify.utils import (
    print_verification_results,
)
from cleared.config.structure import ClearedConfig


def register_verify_command(app: typer.Typer) -> None:
    """
    Register the verify command with the Typer app.

    Args:
        app: The Typer app.

    """

    @app.command("verify")
    def verify_reversed_data(
        config_path: Path = typer.Argument(  # noqa: B008
            ...,
            help="Path to the configuration file",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
        reverse_data_path: Path = typer.Argument(  # noqa: B008
            ...,
            help="Directory path containing the reversed data to verify",
            exists=True,
            file_okay=False,
            dir_okay=True,
        ),
        output_json: Path = typer.Option(  # noqa: B008
            None,
            "--output",
            "-o",
            help="Path to save JSON verification results (optional)",
            file_okay=True,
            dir_okay=False,
        ),
        config_name: str = typer.Option(
            "cleared_config",
            "--config-name",
            "-cn",
            help="Name of the configuration to load",
        ),
        overrides: list[str] | None = typer.Option(  # noqa: B008
            None,
            "--override",
            help="Override configuration values",
        ),
    ) -> None:
        """
        Verify that reversed data matches the original data.

        This command compares the original data (from config input path) with
        the reversed data (from reverse_data_path) column by column and reports
        any mismatches, errors, or warnings.

        Examples:
            cleared verify config.yaml ./reversed_data
            cleared verify config.yaml ./reversed_data -o results.json

        """
        try:
            # Load configuration
            config = load_config_from_file(
                config_path=config_path,
                config_name=config_name,
                overrides=overrides,
            )

            # Run verification
            result = verify_data(
                config,
                reverse_data_path,
            )

            # Print results
            print_verification_results(result)

            # Save JSON if requested
            if output_json:
                # Convert dataclasses to dict for JSON serialization
                result_dict = asdict(result)

                # Ensure all paths are strings and handle NaN values
                def json_serializer(obj):
                    """Serialize special types to JSON-compatible values."""
                    if isinstance(obj, (Path, pd.NA)):
                        return str(obj)
                    if pd.isna(obj):
                        return None
                    raise TypeError(f"Type {type(obj)} not serializable")

                with open(output_json, "w") as f:
                    json.dump(result_dict, f, indent=2, default=json_serializer)
                typer.echo(f"\n💾 Results saved to: {output_json}")

            # Exit with appropriate code
            if result.overview.failed_tables > 0 or result.overview.total_errors > 0:
                raise typer.Exit(code=1)
            elif (
                result.overview.warning_tables > 0 or result.overview.total_warnings > 0
            ):
                raise typer.Exit(code=0)  # Warnings don't fail
            else:
                typer.echo("\n✅ All verifications passed!")
                raise typer.Exit(code=0)

        except Exception as e:
            typer.echo(f"❌ Error during verification: {e}", err=True)
            raise typer.Exit(code=1) from e


def verify_data(
    config: ClearedConfig,
    reverse_data_path: Path,
    load_data_fn: Callable[[ClearedConfig, str, Path], pd.DataFrame | None]
    | None = None,
    get_dropped_columns_fn: Callable[[ClearedConfig, str], set[str]] | None = None,
) -> VerificationResult:
    """
    Verify reversed data against original data using transformer-based comparison.

    Args:
        config: The ClearedConfig object.
        reverse_data_path: The path to the reversed data.
        load_data_fn: Deprecated - not used anymore (kept for backward compatibility)
        get_dropped_columns_fn: Deprecated - not used anymore (kept for backward compatibility)

    Returns:
        A VerificationResult object.

    """
    from cleared.engine import ClearedEngine

    # Create engine from config
    engine = ClearedEngine.from_config(config)

    # Get original data path from config
    original_data_path = Path(config.io.data.input_config.configs["base_path"])
    reverse_data_path = Path(reverse_data_path)

    # Use engine's verify method
    verification_results = engine.verify(
        original_data_path=original_data_path,
        reversed_data_path=reverse_data_path,
    )

    # Convert engine results to VerificationResult model
    # Engine now returns list[ColumnComparisonResult] per table
    table_results: list[TableVerificationResult] = []

    for table_name, column_results in verification_results["table_results"].items():
        # column_results is already a list[ColumnComparisonResult] from the engine

        # Determine table status from column results
        table_status = "pass"
        errors: list[str] = []
        warnings: list[str] = []

        for col_result in column_results:
            if col_result.status == "error":
                table_status = "error"
                errors.append(col_result.message)
            elif col_result.status == "warning":
                if table_status != "error":
                    table_status = "warning"
                warnings.append(col_result.message)

        passed_count = sum(1 for c in column_results if c.status == "pass")
        error_count = sum(1 for c in column_results if c.status == "error")
        warning_count = sum(1 for c in column_results if c.status == "warning")
        total_columns = len(column_results)

        table_results.append(
            TableVerificationResult(
                table_name=table_name,
                status=table_status,
                total_columns=total_columns,
                passed_columns=passed_count,
                error_columns=error_count,
                warning_columns=warning_count,
                errors=errors,
                warnings=warnings,
                column_results=column_results,
            )
        )

    return _prepare_verification_result(table_results, config.name, reverse_data_path)


def verify_table(
    config: ClearedConfig,
    table_name: str,
    original_df: pd.DataFrame,
    reversed_df: pd.DataFrame | None,
    dropped_columns: set[str],
) -> TableVerificationResult:
    """
    Verify a single table by comparing original and reversed data.

    Args:
        config: The ClearedConfig object.
        table_name: The name of the table to verify.
        original_df: The original DataFrame.
        reversed_df: The reversed DataFrame.
        dropped_columns: The columns that were dropped by the ColumnDropper transformer.

    Returns:
        A TableVerificationResult object.

    """
    column_results: list[ColumnComparisonResult] = []
    errors: list[str] = []
    warnings: list[str] = []

    # Get all columns from original
    original_columns = set(original_df.columns)
    reversed_columns = (
        set(reversed_df.columns)
        if reversed_df is not None and not reversed_df.empty
        else set()
    )

    # Compare each column in original
    for column_name in sorted(original_columns):
        original_series = original_df[column_name]
        reversed_series = (
            reversed_df[column_name]
            if reversed_df is not None
            and not reversed_df.empty
            and column_name in reversed_df.columns
            else None
        )

        result = compare_column(
            original_series,
            reversed_series,
            column_name,
            column_name in dropped_columns,
        )
        column_results.append(result)

        if result.status == "error":
            errors.append(result.message)
        elif result.status == "warning":
            warnings.append(result.message)

    # Check for columns in reversed but not in original (shouldn't happen, but check anyway)
    if reversed_df is not None and not reversed_df.empty:
        _handle_extra_columns(
            reversed_df, reversed_columns, original_columns, errors, column_results
        )

    return _prepare_table_verification_result(
        table_name, column_results, errors, warnings
    )


def compare_column(
    original_series: pd.Series,
    reversed_series: pd.Series | None,
    column_name: str,
    is_dropped: bool,
) -> ColumnComparisonResult:
    """
    Compare a single column between original and reversed data.

    Args:
        original_series: The original series.
        reversed_series: The reversed series.
        column_name: The name of the column to compare.
        is_dropped: Whether the column was dropped by the ColumnDropper transformer.

    Returns:
        A ColumnComparisonResult object.

    """
    original_length = len(original_series)

    # Case 1: Column exists in original but not in reversed
    if reversed_series is None:
        return _handle_missing_reversed_column(column_name, original_length, is_dropped)

    reversed_length = len(reversed_series)

    # Case 2: Length mismatch
    if original_length != reversed_length:
        return _handle_length_mismatch(column_name, original_length, reversed_length)

    # Case 3: Compare values index by index
    return _compare_column_values(
        column_name, original_series, reversed_series, original_length, reversed_length
    )


def _compare_column_values(
    column_name: str,
    original_series: pd.Series,
    reversed_series: pd.Series,
    original_length: int,
    reversed_length: int,
) -> ColumnComparisonResult:
    """Compare column values index by index."""
    # Reset index to ensure alignment
    original_aligned = original_series.reset_index(drop=True)
    reversed_aligned = reversed_series.reset_index(drop=True)

    # Compare values - handle NaN properly
    # Two values are equal if:
    # 1. They are both NaN (pandas considers NaN != NaN, so we need special handling)
    # 2. They are both not NaN and equal
    both_nan = original_aligned.isna() & reversed_aligned.isna()
    both_not_nan = ~original_aligned.isna() & ~reversed_aligned.isna()
    values_equal = (original_aligned == reversed_aligned) & both_not_nan

    # Mismatches are rows where values are not equal AND not both NaN
    mismatches = ~(both_nan | values_equal)

    mismatch_count = int(mismatches.sum())

    if mismatch_count == 0:
        return ColumnComparisonResult(
            column_name=column_name,
            status="pass",
            message=f"Column '{column_name}' matches perfectly",
            original_length=original_length,
            reversed_length=reversed_length,
            mismatch_count=0,
            mismatch_percentage=0.0,
        )

    # Calculate mismatch percentage
    mismatch_percentage = (mismatch_count / original_length) * 100.0

    # Get sample mismatch indices (limit to first 100 for JSON size)
    mismatch_indices = original_aligned[mismatches].index.tolist()[:100]

    return ColumnComparisonResult(
        column_name=column_name,
        status="error",
        message=f"Column '{column_name}' has {mismatch_count} mismatches ({mismatch_percentage:.2f}%)",
        original_length=original_length,
        reversed_length=reversed_length,
        mismatch_count=mismatch_count,
        mismatch_percentage=mismatch_percentage,
        sample_mismatch_indices=mismatch_indices,
    )


def _handle_extra_columns(
    reversed_df: pd.DataFrame,
    reversed_columns: set[str],
    original_columns: set[str],
    errors: list[str],
    column_results: list[ColumnComparisonResult],
) -> None:
    """Handle columns that exist in reversed data but not in original data."""
    extra_columns = reversed_columns - original_columns
    if extra_columns:
        for column_name in sorted(extra_columns):
            error_msg = f"Column '{column_name}' exists in reversed data but not in original data"
            errors.append(error_msg)
            column_results.append(
                ColumnComparisonResult(
                    column_name=column_name,
                    status="error",
                    message=error_msg,
                    original_length=0,
                    reversed_length=len(reversed_df[column_name]),
                )
            )


def _prepare_table_verification_result(
    table_name: str,
    column_results: list[ColumnComparisonResult],
    errors: list[str],
    warnings: list[str],
) -> TableVerificationResult:
    """Prepare and return a TableVerificationResult from column results."""
    # Calculate stats
    passed_columns = sum(1 for r in column_results if r.status == "pass")
    error_columns = sum(1 for r in column_results if r.status == "error")
    warning_columns = sum(1 for r in column_results if r.status == "warning")

    # Determine table status
    if error_columns > 0:
        table_status = "error"
    elif warning_columns > 0:
        table_status = "warning"
    else:
        table_status = "pass"

    return TableVerificationResult(
        table_name=table_name,
        status=table_status,
        total_columns=len(column_results),
        passed_columns=passed_columns,
        error_columns=error_columns,
        warning_columns=warning_columns,
        errors=errors,
        warnings=warnings,
        column_results=column_results,
    )


def _prepare_verification_result(
    table_results: list[TableVerificationResult],
    config_name: str,
    reverse_data_path: Path,
) -> VerificationResult:
    """Prepare and return a VerificationResult from table results."""
    # Calculate overview stats
    total_tables = len(table_results)
    passed_tables = sum(1 for r in table_results if r.status == "pass")
    failed_tables = sum(1 for r in table_results if r.status == "error")
    warning_tables = sum(1 for r in table_results if r.status == "warning")

    total_errors = sum(len(r.errors) for r in table_results)
    total_warnings = sum(len(r.warnings) for r in table_results)

    total_columns_checked = sum(r.total_columns for r in table_results)
    total_columns_passed = sum(r.passed_columns for r in table_results)
    total_columns_errored = sum(r.error_columns for r in table_results)
    total_columns_warned = sum(r.warning_columns for r in table_results)

    overview = VerificationOverview(
        total_tables=total_tables,
        passed_tables=passed_tables,
        failed_tables=failed_tables,
        warning_tables=warning_tables,
        total_errors=total_errors,
        total_warnings=total_warnings,
        total_columns_checked=total_columns_checked,
        total_columns_passed=total_columns_passed,
        total_columns_errored=total_columns_errored,
        total_columns_warned=total_columns_warned,
    )

    return VerificationResult(
        overview=overview,
        tables=table_results,
        config_path=str(config_name),
        reverse_data_path=str(reverse_data_path),
    )


def _create_error_status_result(
    table_name: str,
    error_message: str,
) -> TableVerificationResult:
    """Create a TableVerificationResult with error status for data loading failures."""
    # Create a column result for the error to satisfy validation
    error_column_result = ColumnComparisonResult(
        column_name="__table_load_error__",
        status="error",
        message=error_message,
    )

    return TableVerificationResult(
        table_name=table_name,
        status="error",
        total_columns=1,
        passed_columns=0,
        error_columns=1,
        warning_columns=0,
        errors=[error_message],
        warnings=[],
        column_results=[error_column_result],
    )


def _handle_missing_reversed_column(
    column_name: str,
    original_length: int,
    is_dropped: bool,
) -> ColumnComparisonResult:
    """Handle case where column exists in original but not in reversed."""
    if is_dropped:
        return ColumnComparisonResult(
            column_name=column_name,
            status="warning",
            message=f"Column '{column_name}' was dropped by ColumnDropper and is expected to be missing in reversed data",
            original_length=original_length,
            reversed_length=0,
        )
    else:
        return ColumnComparisonResult(
            column_name=column_name,
            status="error",
            message=f"Column '{column_name}' exists in original data but is missing in reversed data",
            original_length=original_length,
            reversed_length=0,
        )


def _handle_length_mismatch(
    column_name: str,
    original_length: int,
    reversed_length: int,
) -> ColumnComparisonResult:
    """Handle case where column lengths don't match."""
    return ColumnComparisonResult(
        column_name=column_name,
        status="error",
        message=f"Column '{column_name}' length mismatch: original has {original_length} rows, reversed has {reversed_length} rows",
        original_length=original_length,
        reversed_length=reversed_length,
    )
