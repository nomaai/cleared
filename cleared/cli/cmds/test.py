"""Test command for running a dry-run test of the ClearedEngine."""

from __future__ import annotations

from pathlib import Path

import typer

from cleared.cli.cmds.run import _run_engine_internal


def register_test_command(app: typer.Typer) -> None:
    """Register the test command with the Typer app."""

    @app.command("test")
    def test_engine(
        config_path: Path = typer.Argument(  # noqa: B008
            ...,
            help="Path to the configuration file",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
        rows: int = typer.Option(
            50,
            "--rows",
            "-r",
            help="Number of rows to process per table (default: 50)",
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
        Test run the ClearedEngine with a limited number of rows (dry run).

        This command runs the same process as 'cleared run' but only processes
        the first N rows of each table and does not write any outputs. This is
        useful for testing your configuration before running on the full dataset.

        Examples:
            cleared test config.yaml
            cleared test config.yaml --rows 50
            cleared test config.yaml -r 100 --verbose

        """
        _run_engine_internal(
            config_path=config_path,
            config_name=config_name,
            overrides=overrides,
            continue_on_error=continue_on_error,
            create_dirs=create_dirs,
            verbose=verbose,
            rows_limit=rows,
            test_mode=True,
        )
