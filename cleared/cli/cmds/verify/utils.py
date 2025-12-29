"""Utility functions for verification."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import typer

from cleared.config.structure import ClearedConfig
from cleared.io.filesystem import FileSystemDataLoader
from cleared.cli.cmds.verify.model import VerificationResult, TableVerificationResult
from omegaconf import DictConfig


def get_column_dropper_columns(config: ClearedConfig, table_name: str) -> set[str]:
    """Get set of column names that are dropped by ColumnDropper transformers."""
    dropped_columns = set()

    if table_name not in config.tables:
        return dropped_columns

    table_config = config.tables[table_name]
    for transformer in table_config.transformers:
        if transformer.method == "ColumnDropper":
            # ColumnDropper uses idconfig.name as the column to drop
            if "idconfig" in transformer.configs:
                idconfig = transformer.configs["idconfig"]
                if isinstance(idconfig, dict) and "name" in idconfig:
                    dropped_columns.add(idconfig["name"])

    return dropped_columns


def _create_loader_for_path(
    config: ClearedConfig,
    data_path: Path,
) -> FileSystemDataLoader | None:
    """
    Create FileSystemDataLoader with overridden base_path.

    Args:
        config: ClearedConfig object
        data_path: Path to override base_path with

    Returns:
        FileSystemDataLoader instance or None if not filesystem type

    """
    input_config = config.io.data.input_config
    if input_config.io_type != "filesystem":
        return None

    loader_config = {
        "data_source_type": "filesystem",
        "connection_params": {
            "base_path": str(data_path),
            "file_format": input_config.configs.get("file_format", "csv"),
        },
    }
    return FileSystemDataLoader(DictConfig(loader_config))


def _load_single_file_table(
    loader: FileSystemDataLoader,
    table_name: str,
    table_path: Path,
) -> pd.DataFrame:
    """Load single file table."""
    return loader.read_table(table_name, segment_path=table_path)


def _load_segment_directory(
    loader: FileSystemDataLoader,
    table_name: str,
    segment_paths: list[Path],
) -> pd.DataFrame:
    """Load and combine segment directory."""
    segment_dfs = []
    for segment_path in segment_paths:
        segment_df = loader.read_table(table_name, segment_path=segment_path)
        segment_dfs.append(segment_df)

    if segment_dfs:
        return pd.concat(segment_dfs, ignore_index=True)
    else:
        return pd.DataFrame()


def load_data_for_table(
    config: ClearedConfig,
    table_name: str,
    data_path: Path,
) -> pd.DataFrame | None:
    """
    Load data for a table from the given path.

    Supports both single files and directories of segment files.
    For directories, combines all segments into a single DataFrame.
    """
    try:
        loader = _create_loader_for_path(config, data_path)
        if loader is None:
            return None

        # Detect if table is single file or directory
        try:
            table_paths = loader.get_table_paths(table_name)
        except Exception:
            return None

        if isinstance(table_paths, Path):
            return _load_single_file_table(loader, table_name, table_paths)
        else:
            return _load_segment_directory(loader, table_name, table_paths)
    except Exception:
        return None


def _print_overview(result: VerificationResult) -> None:
    """Print overview statistics."""
    typer.echo("\n📊 Overview:")
    typer.echo(f"  Total Tables: {result.overview.total_tables}")
    typer.echo(f"  ✅ Passed: {result.overview.passed_tables}")
    typer.echo(f"  ❌ Failed: {result.overview.failed_tables}")
    typer.echo(f"  ⚠️  Warnings: {result.overview.warning_tables}")
    typer.echo(f"  Total Errors: {result.overview.total_errors}")
    typer.echo(f"  Total Warnings: {result.overview.total_warnings}")


def _print_table_errors(table_result: TableVerificationResult) -> None:
    """Print table errors with truncation."""
    if not table_result.errors:
        return

    typer.echo(f"     Errors ({len(table_result.errors)}):")
    for error in table_result.errors[:5]:  # Show first 5 errors
        typer.echo(f"       - {error}")
    if len(table_result.errors) > 5:
        typer.echo(f"       ... and {len(table_result.errors) - 5} more errors")


def _print_table_warnings(table_result: TableVerificationResult) -> None:
    """Print table warnings with truncation."""
    if not table_result.warnings:
        return

    typer.echo(f"     Warnings ({len(table_result.warnings)}):")
    for warning in table_result.warnings[:5]:  # Show first 5 warnings
        typer.echo(f"       - {warning}")
    if len(table_result.warnings) > 5:
        typer.echo(f"       ... and {len(table_result.warnings) - 5} more warnings")


def _print_table_results(result: VerificationResult) -> None:
    """Print per-table results."""
    typer.echo("\n📋 Per-Table Results:")
    for table_result in result.tables:
        status_icon = (
            "✅"
            if table_result.status == "pass"
            else "❌"
            if table_result.status == "error"
            else "⚠️"
        )
        typer.echo(
            f"\n  {status_icon} Table: {table_result.table_name} ({table_result.status})"
        )
        typer.echo(
            f"     Columns: {table_result.passed_columns} passed, {table_result.error_columns} errors, {table_result.warning_columns} warnings"
        )

        _print_table_errors(table_result)
        _print_table_warnings(table_result)


def print_verification_results(result: VerificationResult) -> None:
    """Print verification results to console."""
    typer.echo("\n" + "=" * 60)
    typer.echo("Verification Results")
    typer.echo("=" * 60)

    _print_overview(result)
    _print_table_results(result)
