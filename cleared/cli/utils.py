"""Utility functions for the Cleared CLI."""

from __future__ import annotations

import shutil
import typer
import yaml

from cleared.config.structure import (
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
)

from cleared.config.structure import ClearedConfig
from cleared.config.structure import TimeShiftConfig
from io import StringIO
from pathlib import Path
from ruamel.yaml import YAML
from typing import Any
from hydra.core.global_hydra import GlobalHydra


def load_config_from_file(
    config_path: Path,
    config_name: str = "cleared_config",
    overrides: list | None = None,
) -> ClearedConfig:
    """
    Load a ClearedConfig from a YAML file with support for Hydra-style imports.

    Args:
        config_path: Path to the configuration file
        config_name: Name of the configuration to load (unused for now)
        overrides: List of configuration overrides (unused for now)

    Returns:
        ClearedConfig object

    Raises:
        Exception: If configuration loading fails

    """
    # Convert to Path object if it's a string
    config_path = Path(config_path)
    config_dir = config_path.parent

    # Load the main YAML file
    with open(config_path) as f:
        main_cfg = yaml.safe_load(f)

    # Check if this is a Hydra-style config with defaults
    if "defaults" in main_cfg:
        # Process imports manually
        merged_cfg = _merge_hydra_configs(main_cfg, config_dir)
    else:
        # Regular YAML config
        merged_cfg = main_cfg

    # Convert to ClearedConfig
    return _hydra_to_cleared_config(merged_cfg)


def _merge_hydra_configs(main_cfg: dict, config_dir: Path) -> dict:
    """
    Manually merge Hydra-style configurations by processing defaults.

    Args:
        main_cfg: Main configuration dictionary
        config_dir: Directory containing the config files

    Returns:
        Merged configuration dictionary

    """
    # Start with an empty config
    merged_cfg = {}

    # Process each import in the defaults list first (base configs)
    for import_name in main_cfg.get("defaults", []):
        import_file = config_dir / f"{import_name}.yaml"

        if import_file.exists():
            with open(import_file) as f:
                import_cfg = yaml.safe_load(f)
                # Remove defaults from imported config to avoid recursion
                import_cfg = {k: v for k, v in import_cfg.items() if k != "defaults"}
                # Merge the imported config (base configs merge first)
                merged_cfg = _deep_merge(merged_cfg, import_cfg)
        else:
            print(f"Warning: Import file {import_file} not found, skipping...")

    # Finally, merge the main config on top (main config overrides base configs)
    main_cfg_no_defaults = {k: v for k, v in main_cfg.items() if k != "defaults"}
    merged_cfg = _deep_merge(merged_cfg, main_cfg_no_defaults)

    return merged_cfg


def _deep_merge(dict1: dict, dict2: dict) -> dict:
    """
    Deep merge two dictionaries, with dict2 values taking precedence.

    Args:
        dict1: First dictionary
        dict2: Second dictionary (takes precedence)

    Returns:
        Merged dictionary

    """
    result = dict1.copy()

    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value

    return result


def _hydra_to_cleared_config(cfg: Any) -> ClearedConfig:
    """Convert Hydra config to ClearedConfig object."""
    # Extract deid_config
    deid_config_data = cfg.get("deid_config", {})

    time_shift_data = deid_config_data.get("time_shift")
    time_shift = None
    if time_shift_data:
        # Filter out unsupported fields like 'ref_id' (if any)
        time_shift_dict = {
            k: v for k, v in time_shift_data.items() if k in ["method", "min", "max"]
        }
        time_shift = TimeShiftConfig(**time_shift_dict)

    deid_config = DeIDConfig(time_shift=time_shift)

    # Extract io config
    io_data = cfg.get("io", {})
    data_config = io_data.get("data", {})
    input_config_data = data_config.get("input_config", {})
    input_config = IOConfig(
        io_type=input_config_data.pop("io_type", "filesystem"), **input_config_data
    )
    output_config_data = data_config.get("output_config", {})
    output_config = IOConfig(
        io_type=output_config_data.pop("io_type", "filesystem"), **output_config_data
    )
    data_paired = PairedIOConfig(input_config=input_config, output_config=output_config)

    deid_ref_data = io_data.get("deid_ref", {})
    deid_input_data = deid_ref_data.get("input_config", {})
    deid_input_config = (
        IOConfig(
            io_type=deid_input_data.pop("io_type", "filesystem"), **deid_input_data
        )
        if deid_ref_data.get("input_config")
        else None
    )
    deid_output_data = deid_ref_data.get("output_config", {})
    deid_output_config = IOConfig(
        io_type=deid_output_data.pop("io_type", "filesystem"), **deid_output_data
    )
    deid_ref_paired = PairedIOConfig(
        input_config=deid_input_config, output_config=deid_output_config
    )

    runtime_io_path = io_data.get("runtime_io_path", "/tmp/runtime")
    io_config = ClearedIOConfig(
        data=data_paired, deid_ref=deid_ref_paired, runtime_io_path=runtime_io_path
    )

    # Extract tables
    tables = {}
    for table_name, table_data in cfg.get("tables", {}).items():
        transformers = []
        for transformer_data in table_data.get("transformers", []):
            transformers.append(TransformerConfig(**transformer_data))

        table_config = TableConfig(
            name=table_data.get("name", table_name),
            depends_on=table_data.get("depends_on", []),
            transformers=transformers,
        )
        tables[table_name] = table_config

    # Create ClearedConfig object
    return ClearedConfig(
        name=cfg.get("name", "cleared_engine"),
        deid_config=deid_config,
        io=io_config,
        tables=tables,
    )


def create_sample_config(output_path: Path) -> None:
    """
    Create a sample configuration file by copying from the examples folder.

    Args:
        output_path: Path where to create the sample configuration

    """
    # Get the path to the examples directory
    current_dir = Path(__file__).parent
    examples_dir = current_dir.parent.parent.parent / "examples"
    sample_config_path = examples_dir / "default_config.yaml"

    if sample_config_path.exists():
        # Copy the sample config from examples
        shutil.copy2(sample_config_path, output_path)
        typer.echo(f"Sample configuration created at: {output_path}")
    else:
        # Fallback: create a minimal config if examples file doesn't exist
        minimal_config = """# Sample Cleared Configuration
name: "sample_cleared_engine"

deid_config:
  global_uids: {}
  time_shift: null

io:
  data:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "/tmp/input"
        file_format: "csv"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "/tmp/output"
        file_format: "csv"
  
  deid_ref:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "/tmp/deid_ref_input"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "/tmp/deid_ref_output"
  
  runtime_io_path: "/tmp/runtime"

tables: {}
"""
        output_path.write_text(minimal_config)
        typer.echo(f"Minimal sample configuration created at: {output_path}")


def validate_paths(config: ClearedConfig) -> dict[str, bool]:
    """
    Validate that all required paths exist.

    Args:
        config: ClearedConfig object to validate

    Returns:
        Dictionary mapping path names to their existence status

    """
    paths_to_check = {}

    # Check data input path
    if config.io.data.input_config.io_type == "filesystem":
        input_path = config.io.data.input_config.configs.get("base_path")
        if input_path:
            paths_to_check["data_input"] = Path(input_path).exists()

    # Check data output path
    if config.io.data.output_config.io_type == "filesystem":
        output_path = config.io.data.output_config.configs.get("base_path")
        if output_path:
            paths_to_check["data_output"] = Path(output_path).exists()

    # Check deid_ref input path
    if (
        config.io.deid_ref.input_config
        and config.io.deid_ref.input_config.io_type == "filesystem"
    ):
        deid_input_path = config.io.deid_ref.input_config.configs.get("base_path")
        if deid_input_path:
            paths_to_check["deid_ref_input"] = Path(deid_input_path).exists()

    # Check deid_ref output path
    if config.io.deid_ref.output_config.io_type == "filesystem":
        deid_output_path = config.io.deid_ref.output_config.configs.get("base_path")
        if deid_output_path:
            paths_to_check["deid_ref_output"] = Path(deid_output_path).exists()

    # Check runtime path
    if config.io.runtime_io_path:
        paths_to_check["runtime"] = Path(config.io.runtime_io_path).exists()

    return paths_to_check


def create_missing_directories(config: ClearedConfig) -> None:
    """
    Create missing directories for the configuration.

    Args:
        config: ClearedConfig object

    """
    paths_to_create = []

    # Data input path
    if config.io.data.input_config.io_type == "filesystem":
        input_path = config.io.data.input_config.configs.get("base_path")
        if input_path:
            paths_to_create.append(Path(input_path))

    # Data output path
    if config.io.data.output_config.io_type == "filesystem":
        output_path = config.io.data.output_config.configs.get("base_path")
        if output_path:
            paths_to_create.append(Path(output_path))

    # Deid_ref input path
    if (
        config.io.deid_ref.input_config
        and config.io.deid_ref.input_config.io_type == "filesystem"
    ):
        deid_input_path = config.io.deid_ref.input_config.configs.get("base_path")
        if deid_input_path:
            paths_to_create.append(Path(deid_input_path))

    # Deid_ref output path
    if config.io.deid_ref.output_config.io_type == "filesystem":
        deid_output_path = config.io.deid_ref.output_config.configs.get("base_path")
        if deid_output_path:
            paths_to_create.append(Path(deid_output_path))

    # Runtime path
    if config.io.runtime_io_path:
        paths_to_create.append(Path(config.io.runtime_io_path))

    # Create directories
    for path in paths_to_create:
        path.mkdir(parents=True, exist_ok=True)
        typer.echo(f"Created directory: {path}")


def cleanup_hydra():
    """Clean up Hydra global state."""
    if GlobalHydra().is_initialized():
        GlobalHydra.instance().clear()


def setup_hydra_config_store() -> None:
    """Set up Hydra configuration store with ClearedConfig."""
    from hydra.core.config_store import ConfigStore

    cs = ConfigStore.instance()
    cs.store(name="cleared_config", node=ClearedConfig)


def find_imported_yaml_files(config_path: Path) -> set[Path]:
    """
    Find all YAML files imported by a configuration file via Hydra defaults.

    This function recursively finds all YAML files that are imported through
    the 'defaults' mechanism in Hydra-style configurations.

    Args:
        config_path: Path to the main configuration file

    Returns:
        Set of Path objects for all imported YAML files (including the main file)

    """
    config_path = Path(config_path)
    config_dir = config_path.parent
    found_files: set[Path] = {config_path}
    files_to_process: list[Path] = [config_path]
    processed_files: set[Path] = set()

    while files_to_process:
        current_file = files_to_process.pop(0)

        if current_file in processed_files:
            continue

        processed_files.add(current_file)

        if not current_file.exists():
            continue

        try:
            with open(current_file) as f:
                cfg = yaml.safe_load(f)

            if cfg and isinstance(cfg, dict) and "defaults" in cfg:
                for import_name in cfg.get("defaults", []):
                    import_file = config_dir / f"{import_name}.yaml"

                    if import_file.exists() and import_file not in found_files:
                        found_files.add(import_file)
                        files_to_process.append(import_file)
        except Exception:
            # If we can't read the file, skip it
            continue

    return found_files


def format_yaml_file(file_path: Path, check_only: bool = False) -> bool:
    """
    Format a YAML file using ruamel.yaml to ensure consistent formatting.

    Args:
        file_path: Path to the YAML file to format
        check_only: If True, only check if file needs formatting without modifying it

    Returns:
        True if file was formatted (or would be formatted in check_only mode),
        False if file is already properly formatted

    """
    yaml_obj = YAML()
    yaml_obj.preserve_quotes = True
    yaml_obj.width = 120
    yaml_obj.indent(mapping=2, sequence=4, offset=2)

    try:
        # Read the file
        with open(file_path, encoding="utf-8") as f:
            data = yaml_obj.load(f)

        if data is None:
            return False

        # Write to a temporary string to check if formatting changed
        output = StringIO()
        yaml_obj.dump(data, output)
        formatted_content = output.getvalue()

        # Read original content
        with open(file_path, encoding="utf-8") as f:
            original_content = f.read()

        # Check if formatting is needed
        if original_content.strip() == formatted_content.strip():
            return False

        if check_only:
            return True

        # Write formatted content back to file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(formatted_content)

        return True

    except Exception as e:
        raise ValueError(f"Error formatting {file_path}: {e}") from e
