"""Lint command for linting configuration files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import typer
from yamllint import linter
from yamllint.config import YamlLintConfig

from cleared.cli.utils import (
    load_config_from_file,
    cleanup_hydra,
    setup_hydra_config_store,
)
from cleared.lint.types import LintIssue
from cleared.lint import lint_cleared_config


def register_lint_command(app: typer.Typer) -> None:
    """Register the lint command with the Typer app."""

    @app.command("lint")
    def lint_config(
        config_path: Path = typer.Argument(  # noqa: B008
            ...,
            help="Path to the configuration file to lint",
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
            help="Override configuration values before linting",
        ),
        yamllint_config: Path | None = typer.Option(  # noqa: B008
            None,
            "--yamllint-config",
            help="Path to yamllint configuration file (default: .yamllint)",
        ),
        strict: bool = typer.Option(
            False,
            "--strict",
            "-s",
            help="Treat warnings as errors",
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help="Enable verbose output",
        ),
    ) -> None:
        """
        Lint a Cleared configuration file.

        This command performs both YAML syntax/structure linting (using yamllint)
        and Cleared-specific configuration linting (custom rules).

        Examples:
            cleared lint config.yaml
            cleared lint config.yaml --strict
            cleared lint config.yaml --yamllint-config .custom-yamllint
            cleared lint config.yaml -o "name=test" --verbose

        """
        try:
            setup_hydra_config_store()

            # Find all YAML files (main file + imported sub-files) for YAML syntax linting
            from cleared.cli.utils import find_imported_yaml_files

            yaml_files = find_imported_yaml_files(config_path)
            sorted_yaml_files = sorted(yaml_files)

            if verbose:
                typer.echo(f"\nFound {len(yaml_files)} YAML file(s) to lint:")
                for file in sorted_yaml_files:
                    typer.echo(f"  - {file}")

            yamllint_cfg = _load_yamllint_config(yamllint_config)
            _print_linting_info(config_path, yamllint_config, verbose)

            # Step 1: YAML syntax linting on all files
            yaml_issues = _run_yaml_linting_all_files(
                sorted_yaml_files, yamllint_cfg, verbose
            )

            # Step 2: Cleared-specific linting on final merged config only
            cleared_issues = _run_cleared_linting(
                config_path, yaml_files, config_name, overrides, verbose
            )

            _print_linting_result(yaml_issues, cleared_issues, strict)

        except typer.Exit:
            raise
        except Exception as e:
            _print_error(e, verbose)
            raise typer.Exit(1) from e
        finally:
            cleanup_hydra()


def _load_yamllint_config(yamllint_config: Path | None) -> YamlLintConfig:
    """Load yamllint configuration."""
    yamllint_config_path = yamllint_config or Path(".yamllint")
    if yamllint_config_path.exists():
        return YamlLintConfig(file=str(yamllint_config_path))
    return YamlLintConfig("extends: default")


def _run_yaml_linting_all_files(
    yaml_files: list[Path], yamllint_cfg: YamlLintConfig, verbose: bool
) -> list[LintIssue]:
    """Run yamllint on all YAML files."""
    typer.echo("\n📋 Running YAML linting (yamllint) on all files...")
    yaml_issues: list[LintIssue] = []

    for yaml_file in yaml_files:
        if verbose:
            typer.echo(f"\n  Checking {yaml_file}...")

        try:
            with open(yaml_file, encoding="utf-8") as f:
                yaml_content = f.read()
                yaml_problems = list(linter.run(yaml_content, yamllint_cfg))

            if yaml_problems:
                for problem in yaml_problems:
                    issue = LintIssue(
                        rule=f"yamllint-{problem.rule}",
                        message=f"{yaml_file}: {problem.message}",
                        line=problem.line,
                        severity="error" if problem.level == "error" else "warning",
                    )
                    yaml_issues.append(issue)
                    _print_yaml_issue(yaml_file, problem)
            elif verbose:
                typer.echo(f"  ✅ No YAML syntax issues found in {yaml_file}")
        except Exception as e:
            typer.echo(f"  ⚠️  Error linting {yaml_file}: {e}", err=True)
            import traceback

            typer.echo(traceback.format_exc(), err=True)

    if not yaml_issues:
        typer.echo("  ✅ No YAML syntax issues found in any file")

    return yaml_issues


def _run_cleared_linting(
    config_path: Path,
    yaml_files: set[Path],
    config_name: str,
    overrides: list[str] | None,
    verbose: bool,
) -> list[LintIssue]:
    """
    Run Cleared-specific linting on the final merged ClearedConfig.

    When using Hydra's defaults functionality, sub-files are partial configs
    that get merged into the main config. We should only lint the final merged
    configuration, not individual sub-files.

    Args:
        config_path: Path to the main configuration file
        yaml_files: Set of YAML file paths (for informational purposes)
        config_name: Name of the configuration to load
        overrides: Configuration overrides
        verbose: Enable verbose output

    Returns:
        List of LintIssue objects from Cleared linting

    """
    typer.echo("\n🔍 Running Cleared-specific linting on merged configuration...")

    if verbose:
        typer.echo(f"  Main config: {config_path}")
        if len(yaml_files) > 1:
            typer.echo(f"  Sub-files found: {len(yaml_files) - 1}")
            for yaml_file in sorted(yaml_files):
                if yaml_file != config_path:
                    typer.echo(f"    - {yaml_file}")

    try:
        # Load the final merged ClearedConfig (Hydra will merge all sub-files)
        cleared_config = load_config_from_file(config_path, config_name, overrides)
        cleared_issues = lint_cleared_config(config_path, cleared_config)

        if cleared_issues:
            for issue in cleared_issues:
                _print_cleared_issue(issue)
        else:
            typer.echo("  ✅ No Cleared-specific issues found")

        return cleared_issues
    except Exception as e:
        _print_cleared_linting_error(e, verbose)
        return []


# ============================================================================
# Utility functions for printing/display
# ============================================================================


def _print_linting_info(
    config_path: Path, yamllint_config: Path | None, verbose: bool
) -> None:
    """Print linting information."""
    if verbose:
        typer.echo(f"Linting configuration: {config_path}")
        yamllint_config_path = yamllint_config or Path(".yamllint")
        config_source = (
            yamllint_config_path if yamllint_config_path.exists() else "default"
        )
        typer.echo(f"Using yamllint config: {config_source}")


def _print_yaml_issue(yaml_file: Path, problem: Any) -> None:
    """Print a yamllint issue."""
    icon = "❌" if problem.level == "error" else "⚠️"
    file_display = (
        yaml_file.relative_to(Path.cwd()) if yaml_file.is_absolute() else yaml_file
    )
    typer.echo(
        f"  {icon} {file_display}:{problem.line}: {problem.message} ({problem.rule})"
    )


def _print_cleared_issue(issue: LintIssue) -> None:
    """Print a Cleared linting issue."""
    icon = "❌" if issue.severity == "error" else "⚠️"
    line_str = f" (line {issue.line})" if issue.line else ""
    typer.echo(f"  {icon} [{issue.rule}]{line_str}: {issue.message}")


def _print_cleared_linting_error(error: Exception, verbose: bool) -> None:
    """Print error when Cleared linting fails."""
    typer.echo(f"  ⚠️  Could not run Cleared-specific linting: {error}", err=True)
    import traceback

    typer.echo(traceback.format_exc(), err=True)


def _print_linting_result(
    yaml_issues: list[LintIssue],
    cleared_issues: list[LintIssue],
    strict: bool,
) -> None:
    """Print linting result and raise exit if needed."""
    all_issues = yaml_issues + cleared_issues
    errors = [i for i in all_issues if i.severity == "error"]
    warnings = [i for i in all_issues if i.severity == "warning"]

    typer.echo("\n" + "=" * 60)
    typer.echo("Linting Summary:")
    typer.echo(f"  Errors:   {len(errors)}")
    typer.echo(f"  Warnings: {len(warnings)}")
    typer.echo("=" * 60)

    if errors:
        typer.echo("\n❌ Linting failed with errors")
        raise typer.Exit(1)
    elif warnings and strict:
        typer.echo("\n❌ Linting failed (warnings treated as errors in strict mode)")
        raise typer.Exit(1)
    elif warnings:
        typer.echo("\n⚠️  Linting passed with warnings")
    else:
        typer.echo("\n✅ Linting passed with no issues")


def _print_error(error: Exception, verbose: bool) -> None:
    """Print error message."""
    typer.echo(f"❌ Error: {error}", err=True)
    import traceback

    typer.echo(traceback.format_exc(), err=True)
