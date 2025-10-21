"""Main CLI application for Cleared data de-identification framework."""

from pathlib import Path
from typing import Any

import typer
from hydra.core.config_store import ConfigStore

from cleared.engine import ClearedEngine
from cleared.config.structure import ClearedConfig
from .utils import (
    load_config_from_file,
    create_sample_config,
    validate_paths,
    create_missing_directories,
    cleanup_hydra,
)

# Create the main Typer app
app = typer.Typer(
    name="cleared",
    help="Cleared - A data de-identification framework for Python",
    add_completion=False,
    no_args_is_help=True,
)

# Global variable to store the config store
cs = ConfigStore.instance()


def setup_hydra_config_store():
    """Set up Hydra configuration store with ClearedConfig."""
    cs.store(name="cleared_config", node=ClearedConfig)


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
    try:
        # Set up Hydra configuration store
        setup_hydra_config_store()

        # Load configuration
        cleared_config = load_config_from_file(config_path, config_name, overrides)

        if verbose:
            typer.echo(f"Configuration loaded from: {config_path}")
            typer.echo(f"Overrides applied: {overrides or []}")

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
        _display_results(results, verbose)

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        if verbose:
            import traceback

            typer.echo(traceback.format_exc(), err=True)
            raise typer.Exit(1) from e
    finally:
        # Clean up Hydra
        cleanup_hydra()


def _hydra_to_cleared_config(cfg: Any) -> ClearedConfig:
    """Convert Hydra config to ClearedConfig object."""
    # Extract the configuration data
    config_data = {
        "name": cfg.get("name", "cleared_engine"),
        "deid_config": cfg.get("deid_config", {}),
        "io": cfg.get("io", {}),
        "tables": cfg.get("tables", {}),
    }

    # Create ClearedConfig object
    return ClearedConfig(**config_data)


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


@app.command("validate")
def validate_config(
    config_path: Path = typer.Argument(  # noqa: B008
        ...,
        help="Path to the configuration file to validate",
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
        help="Override configuration values for validation",
    ),
    check_paths: bool = typer.Option(
        True,
        "--check-paths",
        help="Check if required paths exist",
    ),
) -> None:
    """
    Validate a configuration file without running the engine.

    This command loads and validates a configuration file to check for errors
    before running the actual de-identification process.

    Examples:
        cleared validate config.yaml
        cleared validate config.yaml -o "deid_config.global_uids.patient_id.name=patient_id"

    """
    try:
        # Set up Hydra configuration store
        setup_hydra_config_store()

        # Load configuration
        cleared_config = load_config_from_file(config_path, config_name, overrides)

        typer.echo(f"Configuration loaded from: {config_path}")
        typer.echo(f"Overrides applied: {overrides or []}")

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
    finally:
        # Clean up Hydra
        cleanup_hydra()


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
    typer.echo(
        "  cleared run config.yaml -o 'deid_config.global_uids.patient_id.name=patient_id'"
    )
    typer.echo("")
    typer.echo("For more information, visit: https://github.com/nomaai/cleared")


if __name__ == "__main__":
    app()
