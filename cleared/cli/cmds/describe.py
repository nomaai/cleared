"""Describe command for generating HTML configuration reports."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import typer
from jinja2 import Environment, FileSystemLoader, select_autoescape

from cleared.cli.utils import (
    load_config_from_file,
    cleanup_hydra,
    setup_hydra_config_store,
)
from cleared.config.structure import ClearedConfig
import cleared


def register_describe_command(app: typer.Typer) -> None:
    """Register the describe command with the Typer app."""

    @app.command("describe")
    def describe_config(
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
            help="Override configuration values before generating report",
        ),
        output: Path | None = typer.Option(  # noqa: B008
            None,
            "--output",
            "-o",
            help="Output HTML file path (default: describe.html in current directory)",
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help="Enable verbose output",
        ),
    ) -> None:
        """
        Generate an HTML report describing the Cleared configuration.

        This command loads a configuration file and generates a comprehensive
        HTML report with all configuration details, including tables, transformers,
        dependencies, and I/O settings.

        Examples:
            cleared describe config.yaml
            cleared describe config.yaml -o report.html
            cleared describe config.yaml -o /path/to/report.html --verbose

        """
        try:
            setup_hydra_config_store()

            cleared_config = load_config_from_file(config_path, config_name, overrides)
            _print_config_loaded(config_path, overrides, verbose)

            # Determine output path
            output_path = output or Path.cwd() / "describe.html"
            output_path = output_path.resolve()

            # Prepare template data
            template_data = _prepare_template_data(cleared_config, config_path)

            # Generate HTML
            html_content = _generate_html(template_data)

            # Write to file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            _print_success(output_path, verbose)

        except Exception as e:
            _print_error(e, verbose)
            raise typer.Exit(1) from e
        finally:
            cleanup_hydra()


def _prepare_template_data(
    config: ClearedConfig, config_path: Path | None = None
) -> dict:
    """
    Prepare data structure for template rendering.

    Args:
        config: Loaded ClearedConfig object
        config_path: Optional path to the config file

    Returns:
        Dictionary with all data needed for template

    """
    # Calculate overview statistics
    total_transformers = sum(
        len(table_config.transformers) for table_config in config.tables.values()
    )
    total_dependencies = sum(
        len(table_config.depends_on) for table_config in config.tables.values()
    ) + sum(
        len(transformer.depends_on)
        for table_config in config.tables.values()
        for transformer in table_config.transformers
    )

    # Prepare deid_config
    deid_config_data = {}
    if config.deid_config.time_shift:
        deid_config_data["time_shift"] = {
            "method": config.deid_config.time_shift.method,
            "min": config.deid_config.time_shift.min,
            "max": config.deid_config.time_shift.max,
        }

    # Prepare IO config
    io_config_data = {
        "data": {
            "input_config": {
                "io_type": config.io.data.input_config.io_type,
                "suffix": config.io.data.input_config.suffix,
                "configs": config.io.data.input_config.configs or {},
            },
            "output_config": {
                "io_type": config.io.data.output_config.io_type,
                "suffix": config.io.data.output_config.suffix,
                "configs": config.io.data.output_config.configs or {},
            },
        },
        "deid_ref": {
            "input_config": None,
            "output_config": {
                "io_type": config.io.deid_ref.output_config.io_type,
                "suffix": config.io.deid_ref.output_config.suffix,
                "configs": config.io.deid_ref.output_config.configs or {},
            },
        },
        "runtime_path": config.io.runtime_io_path,
    }

    if config.io.deid_ref.input_config:
        io_config_data["deid_ref"]["input_config"] = {
            "io_type": config.io.deid_ref.input_config.io_type,
            "suffix": config.io.deid_ref.input_config.suffix,
            "configs": config.io.deid_ref.input_config.configs or {},
        }

    # Prepare tables data
    tables_data = []
    for table_key, table_config in config.tables.items():
        transformers_data = []
        for transformer in table_config.transformers:
            # Extract column name based on transformer type
            column_name = _extract_column_name(transformer.method, transformer.configs)

            transformer_dict = {
                "method": transformer.method,
                "uid": transformer.uid,
                "depends_on": transformer.depends_on,
                "filter": None,
                "value_cast": transformer.value_cast,
                "configs": transformer.configs,
                "configs_formatted": _format_config_dict(transformer.configs),
                "configs_display": _prepare_config_for_display(transformer.configs),
                "column_name": column_name,
            }

            if transformer.filter:
                transformer_dict["filter"] = {
                    "where_condition": transformer.filter.where_condition,
                    "description": transformer.filter.description,
                }

            transformers_data.append(transformer_dict)

        tables_data.append(
            {
                "key": table_key,
                "name": table_config.name,
                "depends_on": table_config.depends_on,
                "transformers": transformers_data,
            }
        )

    return {
        "config_name": config.name,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": cleared.__version__,
        "config_file": str(config_path) if config_path else None,
        "overview": {
            "table_count": len(config.tables),
            "transformer_count": total_transformers,
            "dependency_count": total_dependencies,
        },
        "deid_config": deid_config_data,
        "io_config": io_config_data,
        "tables": tables_data,
    }


def _format_config_dict(config_dict: dict) -> str:
    """
    Format a configuration dictionary as YAML string.

    Args:
        config_dict: Dictionary to format

    Returns:
        Formatted YAML string representation

    """
    if not config_dict:
        return ""

    try:
        import yaml

        # Format as YAML with proper indentation
        formatted = yaml.dump(
            config_dict, default_flow_style=False, sort_keys=False, allow_unicode=True
        )
        return formatted.rstrip()  # Remove trailing newline
    except ImportError:
        # Fallback to JSON if yaml is not available
        formatted = json.dumps(config_dict, indent=2, default=str)
        return formatted


def _prepare_config_for_display(config_dict: dict) -> list:
    """
    Prepare configuration dictionary for nested bullet point display.

    Args:
        config_dict: Configuration dictionary

    Returns:
        List of dictionaries with 'key', 'value', 'type', and 'children' fields

    """
    if not config_dict:
        return []

    result = []
    for key, value in config_dict.items():
        item = {"key": key, "value": None, "type": None, "children": None}

        if isinstance(value, dict):
            # Nested dictionary - recurse
            item["type"] = "nested"
            item["children"] = _prepare_config_for_display(value)
        elif isinstance(value, list):
            # List - check if it contains dicts or simple values
            if value and len(value) > 0 and isinstance(value[0], dict):
                item["type"] = "nested_list"
                item["children"] = [
                    _prepare_config_for_display(v)
                    if isinstance(v, dict)
                    else [
                        {
                            "key": None,
                            "value": v,
                            "type": _get_value_type(v),
                            "children": None,
                        }
                    ]
                    for v in value
                ]
            else:
                item["type"] = "list"
                item["value"] = value
        else:
            # Simple value
            item["value"] = value
            item["type"] = _get_value_type(value)

        result.append(item)

    return result


def _extract_column_name(method: str, configs: dict) -> str | None:
    """
    Extract the column name being de-identified from transformer configs.

    Args:
        method: Transformer method name
        configs: Transformer configuration dictionary

    Returns:
        Column name or None if not found

    """
    if not configs:
        return None

    # IDDeidentifier uses idconfig.name
    if method == "IDDeidentifier":
        if "idconfig" in configs and isinstance(configs["idconfig"], dict):
            return configs["idconfig"].get("name")
        return None

    # DateTimeDeidentifier uses datetime_column
    if method == "DateTimeDeidentifier":
        return configs.get("datetime_column")

    # ColumnDropper uses idconfig.name or column_name
    if method == "ColumnDropper":
        if "idconfig" in configs and isinstance(configs["idconfig"], dict):
            return configs["idconfig"].get("name")
        return configs.get("column_name")

    # For other transformers, try common patterns
    if "idconfig" in configs and isinstance(configs["idconfig"], dict):
        return configs["idconfig"].get("name")

    return configs.get("column_name") or configs.get("datetime_column")


def _get_value_type(value: Any) -> str:
    """
    Determine the type of a value for styling purposes.

    Args:
        value: The value to check

    Returns:
        Type string: 'hardcoded', 'reference', or 'string'

    """
    if value is None:
        return "hardcoded"

    if isinstance(value, (int, float, bool)):
        return "hardcoded"

    if isinstance(value, str):
        # Check if it looks like a column name, variable, or table name
        # Simple heuristic: if it's lowercase with underscores or camelCase
        if "_" in value or (
            value and value[0].islower() and any(c.isupper() for c in value[1:])
        ):
            return "reference"
        # If it's all uppercase, might be a constant
        if value.isupper():
            return "hardcoded"
        # Default to reference for strings (column names, etc.)
        return "reference"

    return "hardcoded"


def _generate_html(template_data: dict) -> str:
    """
    Generate HTML content from template data.

    Args:
        template_data: Dictionary with data for template

    Returns:
        HTML content as string

    """
    # Get the template directory (relative to this file)
    template_dir = Path(__file__).parent.parent / "templates" / "describe"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )

    template = env.get_template("report.html.j2")
    return template.render(**template_data)


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


def _print_success(output_path: Path, verbose: bool) -> None:
    """Print success message."""
    typer.echo(f"✅ HTML report generated: {output_path}")
    if verbose:
        typer.echo(f"   File size: {output_path.stat().st_size:,} bytes")
        typer.echo(f"   Open in browser: file://{output_path}")


def _print_error(error: Exception, verbose: bool) -> None:
    """Print error message."""
    typer.echo(f"❌ Error generating report: {error}", err=True)
    if verbose:
        import traceback

        typer.echo(traceback.format_exc(), err=True)
