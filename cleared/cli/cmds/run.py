"""Run command for executing the ClearedEngine."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import logging

import typer

from cleared.engine import ClearedEngine
from cleared.logging_config import setup_logging
from cleared.cli.utils import (
    load_config_from_file,
    validate_paths,
    create_missing_directories,
    cleanup_hydra,
    setup_hydra_config_store,
)


def register_run_command(app: typer.Typer) -> None:
    """Register the run command with the Typer app."""

    @app.command("run")
    def run_engine(
        config_path: Path = typer.Argument(  # noqa: B008
            ...,
            help="Path to the configuration file",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
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
            "-o",
            help="Override configuration values (e.g., 'deid_config.global_uids.patient_id.name=patient_id')",
        ),
        continue_on_error: bool = typer.Option(
            False,
            "--continue-on-error",
            "-c",
            help="Continue running remaining pipelines even if one fails",
        ),
        create_dirs: bool = typer.Option(
            False,
            "--create-dirs",
            "-d",
            help="Create missing directories automatically",
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help="Enable verbose output",
        ),
    ) -> None:
        """
        Run the ClearedEngine with the specified configuration file.

        This command loads a configuration file and runs the ClearedEngine with it.
        You can override configuration values using Hydra's override syntax.

        Examples:
            cleared run config.yaml
            cleared run config.yaml -o "deid_config.global_uids.patient_id.name=patient_id"
            cleared run config.yaml -o "io.data.input_config.configs.base_path=/tmp/input" -c

        """
        _run_engine_internal(
            config_path=config_path,
            config_name=config_name,
            overrides=overrides,
            continue_on_error=continue_on_error,
            create_dirs=create_dirs,
            verbose=verbose,
            rows_limit=None,
            test_mode=False,
        )


def _run_engine_internal(
    config_path: Path,
    config_name: str,
    overrides: list[str] | None,
    continue_on_error: bool,
    create_dirs: bool,
    verbose: bool,
    rows_limit: int | None,
    test_mode: bool,
    reverse: bool = False,
    reverse_output_path: Path | None = None,
) -> None:
    """
    Run the engine with optional test mode and reverse mode.

    Args:
        config_path: Path to the configuration file
        config_name: Name of the configuration to load
        overrides: Configuration overrides
        continue_on_error: Continue on error flag
        create_dirs: Create directories flag
        verbose: Verbose output flag
        rows_limit: Optional limit on number of rows to read per table (for testing)
        test_mode: If True, skip writing outputs (dry run mode)
        reverse: If True, run in reverse mode (read from output, write to reverse path)
        reverse_output_path: Directory path for reverse mode output (required if reverse=True)

    """
    # Set up logging level based on verbose flag
    log_level = logging.DEBUG if verbose else logging.INFO
    setup_logging(level=log_level, use_colors=True)

    try:
        setup_hydra_config_store()

        cleared_config = load_config_from_file(config_path, config_name, overrides)
        _print_config_loaded(config_path, overrides, verbose)

        path_status = validate_paths(cleared_config)
        missing_paths = [path for path, exists in path_status.items() if not exists]

        _print_path_validation(missing_paths, create_dirs, verbose)

        if missing_paths and create_dirs:
            create_missing_directories(cleared_config)

        engine = ClearedEngine.__new__(ClearedEngine)
        engine._init_from_config(cleared_config)

        _print_engine_initialized(engine._pipelines, verbose)
        print_run_mode(reverse, test_mode, rows_limit)
        results = engine.run(
            continue_on_error=continue_on_error,
            rows_limit=rows_limit,
            test_mode=test_mode,
            reverse=reverse,
            reverse_output_path=reverse_output_path,
        )

        _display_results(results, verbose)

    except Exception as e:
        _print_error(e, verbose)
        raise typer.Exit(1) from e
    finally:
        cleanup_hydra()


# ============================================================================
# Utility functions for printing/display
# ============================================================================


def print_run_mode(reverse: bool, test_mode: bool, rows_limit: int | None) -> None:
    """
    Print the run mode message.

    Args:
        reverse: If True, run in reverse mode (read from output, write to reverse path)
        test_mode: If True, run in test mode (process first rows_limit rows per table, no outputs)
        rows_limit: Number of rows to process per table (for testing)

    """
    if reverse:
        typer.echo("Starting reverse de-identification process...")
    elif test_mode:
        typer.echo(
            f"Starting test run (processing first {rows_limit} rows per table, no outputs)..."
        )
    else:
        typer.echo("Starting de-identification process...")


def _print_config_loaded(
    config_path: Path, overrides: list[str] | None, verbose: bool
) -> None:
    """Print configuration loaded message."""
    if verbose:
        typer.echo(f"Configuration loaded from: {config_path}")
        typer.echo(f"Overrides applied: {overrides or []}")


def _print_path_validation(
    missing_paths: list[str], create_dirs: bool, verbose: bool
) -> None:
    """Print path validation status and handle missing paths."""
    if verbose:
        typer.echo(f"create_dirs flag: {create_dirs}")
        typer.echo(f"missing_paths: {missing_paths}")

    if missing_paths:
        if create_dirs:
            typer.echo("Creating missing directories...")
        else:
            typer.echo(f"Warning: Missing directories: {', '.join(missing_paths)}")
            typer.echo("Use --create-dirs to create them automatically")


def _print_engine_initialized(pipelines: list, verbose: bool) -> None:
    """Print engine initialization message."""
    if verbose:
        typer.echo(f"Engine initialized with {len(pipelines)} pipelines")
        for i, pipeline in enumerate(pipelines):
            typer.echo(f"  Pipeline {i + 1}: {pipeline.uid}")


def _display_results(results: Any, verbose: bool = False) -> None:
    """Display the results of the engine run."""
    if hasattr(results, "success"):
        if results.success:
            typer.echo("✅ De-identification completed successfully!")
        else:
            typer.echo("❌ De-identification completed with errors")
    else:
        typer.echo("✅ De-identification completed!")

    if verbose and hasattr(results, "results"):
        typer.echo("\nPipeline Results:")
        for pipeline_uid, result in results.results.items():
            status_icon = (
                "✅"
                if result.status == "success"
                else "❌"
                if result.status == "error"
                else "⏭️"
            )
            typer.echo(f"  {status_icon} {pipeline_uid}: {result.status}")
            if result.error:
                typer.echo(f"    Error: {result.error}")

    if verbose and hasattr(results, "execution_order"):
        typer.echo(f"\nExecution Order: {' → '.join(results.execution_order)}")


def _print_error(error: Exception, verbose: bool) -> None:
    """Print error message."""
    from cleared.transformers.base import FormattedDataFrameError

    error_str = str(error)
    is_formatted_error = isinstance(error, FormattedDataFrameError)

    if is_formatted_error:
        # For formatted errors, just print the message without "Error:" prefix or traceback
        typer.echo(error_str)
    else:
        # Check if it's a DataFrame-related error (not using custom exception)
        error_lower = error_str.lower()
        is_dataframe_error = any(
            keyword in error_lower
            for keyword in ["missing column", "not found", "dataframe"]
        )

        if is_dataframe_error:
            # For DataFrame errors, print without "Error:" prefix
            typer.echo(error_str)
        else:
            # For other errors, show full error with traceback
            typer.echo(f"Error: {error}", err=True)
            if verbose:
                import traceback

                typer.echo(traceback.format_exc(), err=True)
