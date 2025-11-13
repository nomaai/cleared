"""Setup command for creating project directories."""

from __future__ import annotations

from pathlib import Path

import typer

from cleared.cli.utils import (
    load_config_from_file,
    validate_paths,
    create_missing_directories,
    cleanup_hydra,
    setup_hydra_config_store,
)
from cleared.config.structure import ClearedConfig


def register_setup_command(app: typer.Typer) -> None:
    """Register the setup command with the Typer app."""

    @app.command("setup")
    def setup_directories(
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
            help="Override configuration values before creating directories",
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help="Enable verbose output",
        ),
    ) -> None:
        """
        Create project directories based on the configuration file.

        This command reads a configuration file and creates all required directories
        for data input/output, de-identification references, and runtime files.

        Examples:
            cleared setup config.yaml
            cleared setup config.yaml -o "io.data.input_config.configs.base_path=/custom/path"
            cleared setup config.yaml --verbose

        """
        try:
            setup_hydra_config_store()

            cleared_config = load_config_from_file(config_path, config_name, overrides)
            _print_config_loaded(config_path, overrides, verbose)

            path_status = validate_paths(cleared_config)
            existing_paths = [path for path, exists in path_status.items() if exists]
            missing_paths = [path for path, exists in path_status.items() if not exists]

            _print_directory_status(path_status, verbose)

            if existing_paths:
                _print_existing_directories(existing_paths)

            if missing_paths:
                _create_and_report_directories(cleared_config, missing_paths)
            else:
                typer.echo("\n✅ All required directories already exist!")

        except Exception as e:
            _print_error(e, verbose)
            raise typer.Exit(1) from e
        finally:
            cleanup_hydra()


def _create_and_report_directories(
    cleared_config: ClearedConfig, missing_paths: list[str]
) -> None:
    """Create missing directories and report results."""
    typer.echo(f"\n📁 Creating missing directories ({len(missing_paths)}):")

    path_mappings = _get_path_mappings(cleared_config)

    # Create directories (suppress individual messages)
    import sys
    from io import StringIO

    old_stdout = sys.stdout
    sys.stdout = StringIO()
    create_missing_directories(cleared_config)
    sys.stdout = old_stdout

    # Show what was created
    created_count = 0
    for path_name in missing_paths:
        if path_name in path_mappings:
            path = path_mappings[path_name]
            if path.exists():
                typer.echo(f"  ✅ {path_name}: {path}")
                created_count += 1
            else:
                typer.echo(f"  ❌ {path_name}: {path} (failed to create)")

    typer.echo(f"\n✅ Successfully created {created_count} directory(ies)")


def _get_path_mappings(cleared_config: ClearedConfig) -> dict[str, Path]:
    """Get path mappings from configuration."""
    path_mappings = {}

    if cleared_config.io.data.input_config.io_type == "filesystem":
        if cleared_config.io.data.input_config.configs.get("base_path"):
            path_mappings["data_input"] = Path(
                cleared_config.io.data.input_config.configs["base_path"]
            )
        if cleared_config.io.data.output_config.configs.get("base_path"):
            path_mappings["data_output"] = Path(
                cleared_config.io.data.output_config.configs["base_path"]
            )

    if (
        cleared_config.io.deid_ref.input_config
        and cleared_config.io.deid_ref.input_config.io_type == "filesystem"
    ):
        if cleared_config.io.deid_ref.input_config.configs.get("base_path"):
            path_mappings["deid_ref_input"] = Path(
                cleared_config.io.deid_ref.input_config.configs["base_path"]
            )

    if cleared_config.io.deid_ref.output_config.io_type == "filesystem":
        if cleared_config.io.deid_ref.output_config.configs.get("base_path"):
            path_mappings["deid_ref_output"] = Path(
                cleared_config.io.deid_ref.output_config.configs["base_path"]
            )

    if cleared_config.io.runtime_io_path:
        path_mappings["runtime"] = Path(cleared_config.io.runtime_io_path)

    return path_mappings


# ============================================================================
# Utility functions for printing/display
# ============================================================================


def _print_config_loaded(
    config_path: Path, overrides: list[str] | None, verbose: bool
) -> None:
    """Print configuration loaded message."""
    if verbose:
        typer.echo(f"Configuration loaded from: {config_path}")
        if overrides:
            typer.echo(f"Overrides applied: {overrides}")


def _print_directory_status(path_status: dict[str, bool], verbose: bool) -> None:
    """Print directory status."""
    if verbose:
        typer.echo("\nDirectory Status:")
        for path_name, exists in path_status.items():
            status = "✅" if exists else "❌"
            typer.echo(f"  {status} {path_name}")


def _print_existing_directories(existing_paths: list[str]) -> None:
    """Print existing directories."""
    typer.echo(f"\n✅ Existing directories ({len(existing_paths)}):")
    for path in existing_paths:
        typer.echo(f"  - {path}")


def _print_error(error: Exception, verbose: bool) -> None:
    """Print error message."""
    typer.echo(f"❌ Error: {error}", err=True)
    import traceback

    typer.echo(traceback.format_exc(), err=True)
