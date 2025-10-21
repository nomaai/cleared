#!/usr/bin/env python3
"""Simple CLI for Cleared data de-identification framework."""

from pathlib import Path

import typer
from cleared.engine import ClearedEngine
from .utils import (
    load_config_from_file,
    create_missing_directories,
    validate_paths,
    create_sample_config,
)

app = typer.Typer(help="Cleared - A data de-identification framework for Python")


@app.command("run")
def run_engine(
    config_path: Path = typer.Argument(..., help="Path to the configuration file"),  # noqa: B008
    continue_on_error: bool = typer.Option(
        False, "--continue-on-error", "-c", help="Continue on error"
    ),
    create_dirs: bool = typer.Option(
        False, "--create-dirs", "-d", help="Create missing directories"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output"
    ),
) -> None:
    """Run the ClearedEngine with the specified configuration file."""
    try:
        # Load configuration
        cleared_config = load_config_from_file(config_path)

        if verbose:
            typer.echo(f"Configuration loaded from: {config_path}")

        # Validate paths
        path_status = validate_paths(cleared_config)
        missing_paths = [path for path, exists in path_status.items() if not exists]

        if verbose:
            typer.echo(f"create_dirs flag: {create_dirs}")
            typer.echo(f"missing_paths: {missing_paths}")

        if missing_paths:
            if create_dirs:
                typer.echo("Creating missing directories...")
                create_missing_directories(cleared_config)
            else:
                typer.echo(f"Warning: Missing directories: {', '.join(missing_paths)}")
                typer.echo("Use --create-dirs to create them automatically")

        # Create and run the engine
        engine = ClearedEngine.__new__(ClearedEngine)
        engine._init_from_config(cleared_config)

        if verbose:
            typer.echo(f"Engine initialized with {len(engine._pipelines)} pipelines")
            for i, pipeline in enumerate(engine._pipelines):
                typer.echo(f"  Pipeline {i + 1}: {pipeline.uid}")

        # Run the engine
        typer.echo("Starting de-identification process...")
        results = engine.run(continue_on_error=continue_on_error)

        # Display results
        if results.success:
            typer.echo("✅ De-identification completed successfully!")
        else:
            typer.echo("❌ De-identification completed with errors")

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

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback

            typer.echo(traceback.format_exc(), err=True)
            raise typer.Exit(1) from e


@app.command("validate")
def validate_config(
    config_path: Path = typer.Argument(..., help="Path to the configuration file"),  # noqa: B008
    check_paths: bool = typer.Option(
        True, "--check-paths", help="Check if required paths exist"
    ),
) -> None:
    """Validate a configuration file without running the engine."""
    try:
        # Load configuration
        cleared_config = load_config_from_file(config_path)

        typer.echo(f"Configuration loaded from: {config_path}")

        # Validate the configuration by creating an engine instance
        engine = ClearedEngine.__new__(ClearedEngine)
        engine._init_from_config(cleared_config)

        typer.echo("✅ Configuration is valid!")
        typer.echo(
            f"Engine would be initialized with {len(engine._pipelines)} pipelines"
        )

        # Check paths if requested
        if check_paths:
            path_status = validate_paths(cleared_config)
            missing_paths = [path for path, exists in path_status.items() if not exists]

            if missing_paths:
                typer.echo(f"⚠️  Missing directories: {', '.join(missing_paths)}")
            else:
                typer.echo("✅ All required directories exist")

    except Exception as e:
        typer.echo(f"❌ Configuration validation failed: {e}", err=True)
        raise typer.Exit(1) from e


@app.command("init")
def init_project(
    output_path: Path = typer.Argument(  # noqa: B008
        "sample_config.yaml", help="Path where to create the sample configuration file"
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing file"),
) -> None:
    """Initialize a new Cleared project with a sample configuration file."""
    try:
        # Check if file already exists
        if output_path.exists() and not force:
            typer.echo(
                f"Error: File {output_path} already exists. Use --force to overwrite.",
                err=True,
            )
            raise typer.Exit(1)

        # Create the sample configuration
        create_sample_config(output_path)

        typer.echo("")
        typer.echo("Next steps:")
        typer.echo(f"1. Edit the configuration file: {output_path}")
        typer.echo("2. Update the paths in the configuration to match your setup")
        typer.echo(f"3. Validate the configuration: cleared validate {output_path}")
        typer.echo(f"4. Run the de-identification: cleared run {output_path}")

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


@app.command("info")
def show_info() -> None:
    """Show information about the Cleared framework."""
    typer.echo("Cleared - A data de-identification framework for Python")
    typer.echo("=" * 50)
    typer.echo("Version: 0.1.0")
    typer.echo("Author: NOMA AI INC.")
    typer.echo("License: Apache-2.0")
    typer.echo("")
    typer.echo("Available commands:")
    typer.echo("  run       - Run the de-identification engine")
    typer.echo("  validate  - Validate a configuration file")
    typer.echo("  init      - Initialize a new project with sample config")
    typer.echo("  info      - Show this information")
    typer.echo("")
    typer.echo("Examples:")
    typer.echo("  cleared init                           # Create sample config")
    typer.echo("  cleared validate config.yaml          # Validate config")
    typer.echo("  cleared run config.yaml               # Run de-identification")
    typer.echo("  cleared run config.yaml --create-dirs # Create missing directories")


if __name__ == "__main__":
    app()
