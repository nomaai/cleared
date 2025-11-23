"""Info command for displaying framework information."""

from __future__ import annotations

import typer


def register_info_command(app: typer.Typer) -> None:
    """Register the info command with the Typer app."""

    @app.command("info")
    def show_info() -> None:
        """Show information about the Cleared framework."""
        _print_header()
        _print_commands()
        _print_examples()
        _print_footer()


# ============================================================================
# Utility functions for printing/display
# ============================================================================


def _print_header() -> None:
    """Print header information."""
    typer.echo("Cleared - A data de-identification framework for Python")
    typer.echo("=" * 50)
    typer.echo("Version: 0.1.0")
    typer.echo("Author: NOMA AI INC.")
    typer.echo("License: Apache-2.0 with Commons Clause restriction")
    typer.echo("")


def _print_commands() -> None:
    """Print available commands."""
    typer.echo("Available commands:")
    typer.echo("  run         - Run the de-identification engine")
    typer.echo("  test        - Test run with limited rows (dry run, no outputs)")
    typer.echo("  validate    - Validate a configuration file (check-syntax + lint)")
    typer.echo("  check-syntax - Check configuration syntax and structure")
    typer.echo("  lint        - Lint a configuration file (YAML + Cleared rules)")
    typer.echo("  format      - Format YAML configuration files and imported sub-files")
    typer.echo("  setup       - Create project directories from config")
    typer.echo("  init        - Initialize a new project with sample config")
    typer.echo("  describe    - Generate HTML description of the configuration")
    typer.echo("  info        - Show this information")
    typer.echo("")


def _print_examples() -> None:
    """Print usage examples."""
    typer.echo("Examples:")
    typer.echo("  cleared init                           # Create sample config")
    typer.echo(
        "  cleared validate config.yaml          # Full validation (syntax + lint)"
    )
    typer.echo("  cleared check-syntax config.yaml      # Check syntax only")
    typer.echo("  cleared lint config.yaml              # Lint only")
    typer.echo("  cleared format config.yaml            # Format YAML files")
    typer.echo("  cleared setup config.yaml            # Create directories")
    typer.echo("  cleared test config.yaml              # Test with 10 rows (dry run)")
    typer.echo("  cleared test config.yaml --rows 50    # Test with 50 rows")
    typer.echo("  cleared run config.yaml               # Run de-identification")
    typer.echo(
        "  cleared run config.yaml -o 'deid_config.global_uids.patient_id.name=patient_id'"
    )
    typer.echo("")


def _print_footer() -> None:
    """Print footer information."""
    typer.echo("For more information, visit: https://github.com/nomaai/cleared")
