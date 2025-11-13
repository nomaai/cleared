"""Validate command for comprehensive configuration validation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import typer

from cleared.engine import ClearedEngine
from cleared.cli.utils import (
    load_config_from_file,
    validate_paths,
    cleanup_hydra,
    setup_hydra_config_store,
    find_imported_yaml_files,
)
from cleared.lint.types import LintIssue
from cleared.lint import lint_cleared_config
from yamllint import linter
from yamllint.config import YamlLintConfig


def register_validate_command(app: typer.Typer) -> None:
    """Register the validate command with the Typer app."""

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
        Validate a configuration file (runs check-syntax and lint).

        This command performs comprehensive validation by:
        1. Checking configuration syntax and structure (check-syntax)
        2. Linting the configuration file (lint)

        Examples:
            cleared validate config.yaml
            cleared validate config.yaml --strict
            cleared validate config.yaml -o "name=test" --verbose

        """
        syntax_check_passed = False
        try:
            # Step 1: Check syntax
            typer.echo("🔍 Step 1: Checking configuration syntax...")
            _check_syntax_internal(
                config_path=config_path,
                config_name=config_name,
                overrides=overrides,
                check_paths=check_paths,
            )
            typer.echo("\n✅ Syntax check passed\n")
            syntax_check_passed = True
        except Exception as e:
            typer.echo("\n❌ Validation failed at syntax check step")
            typer.echo("⚠️  Skipping lint step because syntax check failed")
            import traceback

            typer.echo(f"\nError details: {e}", err=True)
            typer.echo(traceback.format_exc(), err=True)
            raise typer.Exit(1) from e
        finally:
            cleanup_hydra()

        # Only run lint if syntax check passed
        if syntax_check_passed:
            try:
                # Step 2: Lint
                typer.echo("🔍 Step 2: Linting configuration...")
                _lint_internal(
                    config_path=config_path,
                    config_name=config_name,
                    overrides=overrides,
                    yamllint_config=yamllint_config,
                    strict=strict,
                    verbose=verbose,
                )
                typer.echo("\n✅ Linting passed\n")
            except typer.Exit:
                typer.echo("\n❌ Validation failed at linting step")
                raise
            finally:
                cleanup_hydra()

            typer.echo("=" * 60)
            typer.echo("✅ Validation completed successfully!")
            typer.echo("=" * 60)


def _check_syntax_internal(
    config_path: Path,
    config_name: str,
    overrides: list[str] | None,
    check_paths: bool,
) -> None:
    """Check syntax internally (used by validate command)."""
    # Set up Hydra configuration store
    setup_hydra_config_store()

    # Load configuration
    cleared_config = load_config_from_file(config_path, config_name, overrides)

    _print_config_loaded(config_path, overrides)

    # Validate the configuration by creating an engine instance
    engine = ClearedEngine.__new__(ClearedEngine)
    engine._init_from_config(cleared_config)

    _print_config_valid(len(engine._pipelines))

    # Check paths if requested
    if check_paths:
        path_status = validate_paths(cleared_config)
        missing_paths = [path for path, exists in path_status.items() if not exists]
        _print_path_status(missing_paths)


def _run_yaml_linting(
    yaml_files: set[Path],
    yamllint_config: Path | None,
    verbose: bool,
) -> list[LintIssue]:
    """
    Run yamllint on all YAML files.

    Args:
        yaml_files: Set of YAML file paths to lint
        yamllint_config: Path to yamllint config file (None for default)
        verbose: Enable verbose output

    Returns:
        List of LintIssue objects from yamllint

    """
    sorted_yaml_files = sorted(yaml_files)

    if verbose:
        _print_files_found(yaml_files, sorted_yaml_files)

    # Load yamllint config
    yamllint_config_path = yamllint_config or Path(".yamllint")
    if yamllint_config_path.exists():
        yamllint_cfg = YamlLintConfig(file=str(yamllint_config_path))
    else:
        yamllint_cfg = YamlLintConfig("extends: default")

    typer.echo("\n📋 Running YAML linting (yamllint) on all files...")
    yaml_issues: list[LintIssue] = []

    for yaml_file in sorted_yaml_files:
        if verbose:
            _print_checking_file(yaml_file)

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
                _print_no_issues_in_file(yaml_file, "YAML syntax")
        except Exception as e:
            _print_linting_error(yaml_file, e, verbose)

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
                _print_cleared_issue(config_path, issue)
        else:
            typer.echo("  ✅ No Cleared-specific issues found")

        return cleared_issues
    except Exception as e:
        _print_cleared_linting_error(config_path, e, verbose)
        return []


def _lint_internal(
    config_path: Path,
    config_name: str,
    overrides: list[str] | None,
    yamllint_config: Path | None,
    strict: bool,
    verbose: bool,
) -> None:
    """Lint internally (used by validate command)."""
    # Set up Hydra configuration store
    setup_hydra_config_store()

    # Find all YAML files (main file + imported sub-files)
    yaml_files = find_imported_yaml_files(config_path)

    # Step 1: Run yamllint on all files
    yaml_issues = _run_yaml_linting(yaml_files, yamllint_config, verbose)

    # Step 2: Run Cleared-specific linting on all files
    cleared_issues = _run_cleared_linting(
        config_path, yaml_files, config_name, overrides, verbose
    )

    # Summary and result
    all_issues = yaml_issues + cleared_issues
    errors = [i for i in all_issues if i.severity == "error"]
    warnings = [i for i in all_issues if i.severity == "warning"]

    _print_linting_summary(len(yaml_files), len(errors), len(warnings))
    _print_linting_result(errors, warnings, strict)


# ============================================================================
# Utility functions for printing/display
# ============================================================================


def _print_config_loaded(config_path: Path, overrides: list[str] | None) -> None:
    """Print configuration loaded message."""
    typer.echo(f"Configuration loaded from: {config_path}")
    if overrides:
        typer.echo(f"Overrides applied: {overrides}")


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


def _print_files_found(yaml_files: set[Path], sorted_yaml_files: list[Path]) -> None:
    """Print list of files found for linting."""
    typer.echo(f"\nFound {len(yaml_files)} YAML file(s) to lint:")
    for file in sorted_yaml_files:
        typer.echo(f"  - {file}")


def _print_checking_file(yaml_file: Path) -> None:
    """Print message indicating which file is being checked."""
    typer.echo(f"\n  Checking {yaml_file}...")


def _print_yaml_issue(yaml_file: Path, problem: Any) -> None:
    """Print a yamllint issue."""
    icon = "❌" if problem.level == "error" else "⚠️"
    file_display = (
        yaml_file.relative_to(Path.cwd()) if yaml_file.is_absolute() else yaml_file
    )
    typer.echo(
        f"  {icon} {file_display}:{problem.line}: {problem.message} ({problem.rule})"
    )


def _print_cleared_issue(config_path: Path, issue: LintIssue) -> None:
    """Print a Cleared linting issue."""
    icon = "❌" if issue.severity == "error" else "⚠️"
    line_str = f" (line {issue.line})" if issue.line else ""
    file_display = (
        config_path.relative_to(Path.cwd())
        if config_path.is_absolute()
        else config_path
    )
    # Note: issue.message may already include file path from lint_cleared_config
    typer.echo(f"  {icon} {file_display} [{issue.rule}]{line_str}: {issue.message}")


def _print_no_issues_in_file(yaml_file: Path, lint_type: str) -> None:
    """Print message when no issues found in a file."""
    typer.echo(f"  ✅ No {lint_type} issues found in {yaml_file}")


def _print_linting_error(yaml_file: Path, error: Exception, verbose: bool) -> None:
    """Print error message when linting a file fails."""
    typer.echo(f"  ⚠️  Error linting {yaml_file}: {error}", err=True)
    import traceback

    typer.echo(traceback.format_exc(), err=True)


def _print_cleared_linting_error(
    config_path: Path, error: Exception, verbose: bool
) -> None:
    """Print error message when Cleared linting fails."""
    typer.echo(
        f"  ⚠️  Could not run Cleared-specific linting on {config_path}: {error}",
        err=True,
    )
    import traceback

    typer.echo(traceback.format_exc(), err=True)


def _print_linting_summary(
    file_count: int, error_count: int, warning_count: int
) -> None:
    """Print linting summary."""
    typer.echo("\n" + "=" * 60)
    typer.echo("Linting Summary:")
    typer.echo(f"  Files checked: {file_count}")
    typer.echo(f"  Errors:   {error_count}")
    typer.echo(f"  Warnings: {warning_count}")
    typer.echo("=" * 60)


def _print_linting_result(
    errors: list[LintIssue], warnings: list[LintIssue], strict: bool
) -> None:
    """Print linting result and raise exit if needed."""
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
