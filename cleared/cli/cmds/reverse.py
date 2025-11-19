"""Reverse command for reversing de-identification process."""

from __future__ import annotations

from pathlib import Path

import typer

from cleared.cli.cmds.run import _run_engine_internal


def register_reverse_command(app: typer.Typer) -> None:
    """Register the reverse command with the Typer app."""

    @app.command("reverse")
    def reverse_engine(
        config_path: Path = typer.Argument(  # noqa: B008
            ...,
            help="Path to the configuration file",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
        reverse_output_path: Path = typer.Option(  # noqa: B008
            ...,
            "--output",
            "-o",
            help="Directory path where reversed data will be written",
            exists=False,
            file_okay=False,
            dir_okay=True,
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
        Reverse the de-identification process.

        This command reads de-identified data from the output configuration
        and reverses the transformations to restore original values, writing
        the results to the specified reverse output directory.

        Examples:
            cleared reverse config.yaml -o ./reversed_data
            cleared reverse config.yaml -o ./reversed_data --verbose

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
            reverse=True,
            reverse_output_path=reverse_output_path,
        )
