"""Format command for formatting YAML configuration files."""

from __future__ import annotations

from pathlib import Path

import traceback
import typer

from cleared.cli.utils import find_imported_yaml_files, format_yaml_file


def register_format_command(app: typer.Typer) -> None:
    """Register the format command with the Typer app."""

    @app.command("format")
    def format_config(
        config_path: Path = typer.Argument(  # noqa: B008
            ...,
            help="Path to the configuration file to format",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
        check: bool = typer.Option(
            False,
            "--check",
            "-c",
            help="Check if files need formatting without modifying them",
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help="Enable verbose output",
        ),
    ) -> None:
        """
        Format YAML configuration files and all imported sub-files.

        This command formats the main configuration file and all YAML files
        imported via Hydra's defaults mechanism using consistent YAML formatting.

        Examples:
            cleared format config.yaml
            cleared format config.yaml --check
            cleared format config.yaml --verbose

        """
        try:
            yaml_files = find_imported_yaml_files(config_path)
            _print_files_found(yaml_files, verbose)

            if check:
                _run_format_check(yaml_files, verbose)
            else:
                _run_format_files(yaml_files, verbose)

        except ImportError as e:
            _print_import_error(e)
            raise typer.Exit(1) from e
        except Exception as e:
            _print_error(e, verbose)
            raise typer.Exit(1) from e


def _run_format_check(yaml_files: set[Path], verbose: bool) -> None:
    """Run format check on all files."""
    typer.echo("\n🔍 Checking YAML file formatting...")
    needs_formatting: list[Path] = []

    for yaml_file in sorted(yaml_files):
        try:
            if format_yaml_file(yaml_file, check_only=True):
                needs_formatting.append(yaml_file)
                _print_file_needs_formatting(yaml_file, verbose)
        except Exception as e:
            _print_format_error(yaml_file, e)

    if needs_formatting:
        _print_format_check_failed(needs_formatting)
        raise typer.Exit(1)
    else:
        typer.echo("\n✅ All files are properly formatted!")


def _run_format_files(yaml_files: set[Path], verbose: bool) -> None:
    """Run format on all files."""
    typer.echo("\n✨ Formatting YAML files...")
    formatted_count = 0

    for yaml_file in sorted(yaml_files):
        try:
            if format_yaml_file(yaml_file, check_only=False):
                formatted_count += 1
                _print_file_formatted(yaml_file, verbose)
            elif verbose:
                _print_file_already_formatted(yaml_file)
        except Exception as e:
            _print_format_error(yaml_file, e)
            raise typer.Exit(1) from e

    if formatted_count > 0:
        typer.echo(f"\n✅ Formatted {formatted_count} file(s)")
    else:
        typer.echo("\n✅ All files are already properly formatted!")


# ============================================================================
# Utility functions for printing/display
# ============================================================================


def _print_files_found(yaml_files: set[Path], verbose: bool) -> None:
    """Print list of files found for formatting."""
    if verbose:
        typer.echo(f"Found {len(yaml_files)} YAML file(s) to format:")
        for file in sorted(yaml_files):
            typer.echo(f"  - {file}")


def _print_file_needs_formatting(yaml_file: Path, verbose: bool) -> None:
    """Print message when file needs formatting."""
    if verbose:
        typer.echo(f"  ⚠️  {yaml_file} needs formatting")


def _print_file_formatted(yaml_file: Path, verbose: bool) -> None:
    """Print message when file is formatted."""
    if verbose:
        typer.echo(f"  ✅ Formatted: {yaml_file}")


def _print_file_already_formatted(yaml_file: Path) -> None:
    """Print message when file is already formatted."""
    typer.echo(f"  ✓ Already formatted: {yaml_file}")


def _print_format_error(yaml_file: Path, error: Exception) -> None:
    """Print error when formatting fails."""
    typer.echo(f"  ❌ Error checking {yaml_file}: {error}", err=True)
    typer.echo(traceback.format_exc(), err=True)


def _print_format_check_failed(needs_formatting: list[Path]) -> None:
    """Print format check failed message."""
    typer.echo(f"\n❌ {len(needs_formatting)} file(s) need formatting:")
    for file in needs_formatting:
        typer.echo(f"  - {file}")
    typer.echo("\nRun 'cleared format' without --check to format them.")


def _print_import_error(error: ImportError) -> None:
    """Print import error message."""
    typer.echo(f"❌ {error}", err=True)
    typer.echo(traceback.format_exc(), err=True)
    typer.echo(
        "\n💡 Install ruamel.yaml to enable formatting:\n   pip install ruamel.yaml",
        err=True,
    )


def _print_error(error: Exception, verbose: bool) -> None:
    """Print error message."""
    typer.echo(f"❌ Error: {error}", err=True)
    typer.echo(traceback.format_exc(), err=True)
