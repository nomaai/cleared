"""Check-syntax command for validating configuration syntax."""

from __future__ import annotations

from pathlib import Path

import typer

from cleared.engine import ClearedEngine
from cleared.cli.utils import (
    load_config_from_file,
    validate_paths,
    cleanup_hydra,
    setup_hydra_config_store,
)


def register_check_syntax_command(app: typer.Typer) -> None:
    """Register the check-syntax command with the Typer app."""

    @app.command("check-syntax")
    def check_syntax_config(
        config_path: Path = typer.Argument(  # noqa: B008
            ...,
            help="Path to the configuration file to check",
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
            help="Override configuration values for checking",
        ),
        check_paths: bool = typer.Option(
            True,
            "--check-paths",
            help="Check if required paths exist",
        ),
    ) -> None:
        """
        Check configuration file syntax and structure without running the engine.

        This command loads and checks a configuration file to verify it can be
        loaded and initialized before running the actual de-identification process.

        Examples:
            cleared check-syntax config.yaml
            cleared check-syntax config.yaml -o "deid_config.global_uids.patient_id.name=patient_id"

        """
        try:
            setup_hydra_config_store()

            cleared_config = load_config_from_file(config_path, config_name, overrides)
            _print_config_loaded(config_path, overrides)

            engine = ClearedEngine.__new__(ClearedEngine)
            engine._init_from_config(cleared_config)

            _print_config_valid(len(engine._pipelines))

            if check_paths:
                path_status = validate_paths(cleared_config)
                missing_paths = [
                    path for path, exists in path_status.items() if not exists
                ]
                _print_path_status(missing_paths)

        except Exception as e:
            _print_syntax_check_failed(e)
            raise typer.Exit(1) from e
        finally:
            cleanup_hydra()


# ============================================================================
# Utility functions for printing/display
# ============================================================================


def _print_config_loaded(config_path: Path, overrides: list[str] | None) -> None:
    """Print configuration loaded message."""
    typer.echo(f"Configuration loaded from: {config_path}")
    typer.echo(f"Overrides applied: {overrides or []}")


def _print_config_valid(pipeline_count: int) -> None:
    """Print configuration valid message."""
    typer.echo("✅ Configuration is valid!")
    typer.echo(f"Engine would be initialized with {pipeline_count} pipelines")


def _print_path_status(missing_paths: list[str]) -> None:
    """Print path status message."""
    if missing_paths:
        typer.echo(f"⚠️  Missing directories: {', '.join(missing_paths)}")
    else:
        typer.echo("✅ All required directories exist")


def _print_syntax_check_failed(error: Exception) -> None:
    """Print syntax check failed message."""
    typer.echo(f"❌ Configuration syntax check failed: {error}", err=True)
    import traceback

    typer.echo(traceback.format_exc(), err=True)
