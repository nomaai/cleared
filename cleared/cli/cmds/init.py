"""Init command for initializing a new Cleared project."""

from __future__ import annotations

from pathlib import Path

import typer

from cleared.cli.utils import create_sample_config


def register_init_command(app: typer.Typer) -> None:
    """Register the init command with the Typer app."""

    @app.command("init")
    def init_project(
        output_path: Path = typer.Argument(  # noqa: B008
            "sample_config.yaml",
            help="Path where to create the sample configuration file",
        ),
        force: bool = typer.Option(
            False,
            "--force",
            "-f",
            help="Overwrite existing file if it exists",
        ),
    ) -> None:
        """
        Initialize a new Cleared project with a sample configuration file.

        This command creates a sample configuration file that you can use as a starting
        point for your de-identification project.

        Examples:
            cleared init
            cleared init my_config.yaml
            cleared init config.yaml --force

        """
        try:
            if output_path.exists() and not force:
                _print_file_exists_error(output_path)
                raise typer.Exit(1)

            create_sample_config(output_path)
            _print_next_steps(output_path)

        except Exception as e:
            _print_error(e)
            raise typer.Exit(1) from e


# ============================================================================
# Utility functions for printing/display
# ============================================================================


def _print_file_exists_error(output_path: Path) -> None:
    """Print file exists error message."""
    typer.echo(
        f"Error: File {output_path} already exists. Use --force to overwrite.",
        err=True,
    )


def _print_next_steps(output_path: Path) -> None:
    """Print next steps after initialization."""
    typer.echo("")
    typer.echo("Next steps:")
    typer.echo(f"1. Edit the configuration file: {output_path}")
    typer.echo("2. Update the paths in the configuration to match your setup")
    typer.echo(f"3. Validate the configuration: cleared validate {output_path}")
    typer.echo(f"4. Run the de-identification: cleared run {output_path}")


def _print_error(error: Exception) -> None:
    """Print error message."""
    typer.echo(f"Error: {error}", err=True)
    import traceback

    typer.echo(traceback.format_exc(), err=True)
