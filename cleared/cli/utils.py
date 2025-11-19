"""Utility functions for the Cleared CLI."""

from __future__ import annotations

import shutil
import typer
import yaml

from cleared.config.structure import (
    DeIDConfig,
    ClearedIOConfig,
    ClearedConfig,
    IOConfig,
    PairedIOConfig,
    TableConfig,
    TransformerConfig,
    IdentifierConfig,
    TimeShiftConfig,
    FilterConfig,
)
from io import StringIO
from pathlib import Path
from ruamel.yaml import YAML
from hydra.core.global_hydra import GlobalHydra


def _initialize_config_store() -> None:
    """
    Initialize Hydra ConfigStore with all Cleared configuration classes.

    This registers all dataclasses from structure.py with Hydra's ConfigStore
    so they can be used as structured configs.
    """
    from hydra.core.config_store import ConfigStore

    cs = ConfigStore.instance()

    # Register all configuration classes
    cs.store(name="cleared_config", node=ClearedConfig)
    cs.store(name="identifier_config", node=IdentifierConfig)
    cs.store(name="time_shift_config", node=TimeShiftConfig)
    cs.store(name="deid_config", node=DeIDConfig)
    cs.store(name="filter_config", node=FilterConfig)
    cs.store(name="transformer_config", node=TransformerConfig)
    cs.store(name="table_config", node=TableConfig)
    cs.store(name="io_config", node=IOConfig)
    cs.store(name="paired_io_config", node=PairedIOConfig)
    cs.store(name="cleared_io_config", node=ClearedIOConfig)


def load_config_from_file(
    config_path: Path,
    config_name: str = "cleared_config",
    overrides: list | None = None,
) -> ClearedConfig:
    """
    Load a ClearedConfig from a YAML file with support for Hydra-style imports.

    Uses Hydra's compose API with structured configs to directly return ClearedConfig objects.

    Args:
        config_path: Path to the configuration file
        config_name: Name of the configuration to load (defaults to filename without extension)
        overrides: List of configuration overrides in Hydra format (e.g., ["key=value", "group.key=value"])

    Returns:
        ClearedConfig object

    Raises:
        Exception: If configuration loading fails

    """
    from hydra import compose, initialize_config_dir
    from hydra.core.global_hydra import GlobalHydra
    from hydra.utils import instantiate
    from omegaconf import OmegaConf

    # Convert to Path object if it's a string
    config_path = Path(config_path)
    config_dir = config_path.parent
    config_file_stem = config_path.stem

    # Determine config name from filename if using default
    if config_name == "cleared_config":
        config_name = config_file_stem

    # Clean up any existing Hydra instance
    if GlobalHydra().is_initialized():
        GlobalHydra.instance().clear()

    # Initialize ConfigStore with all config classes
    _initialize_config_store()

    try:
        # Initialize Hydra with the config directory
        with initialize_config_dir(config_dir=str(config_dir), version_base=None):
            # Compose the configuration with overrides
            overrides_list = overrides if overrides else []
            cfg = compose(config_name=config_name, overrides=overrides_list)

        # Convert OmegaConf to ClearedConfig using structured configs
        # Merge the config with the structured config schema to get proper defaults
        structured_cfg = OmegaConf.structured(ClearedConfig)
        # Merge the actual config data with the structured schema
        merged_cfg = OmegaConf.merge(structured_cfg, cfg)
        # Use instantiate with _convert_="object" to convert to actual dataclass instance
        return instantiate(merged_cfg, _convert_="object")
    except Exception:
        # Fallback to manual loading if Hydra fails (e.g., for non-Hydra configs)
        # This maintains backward compatibility
        with open(config_path) as f:
            main_cfg = yaml.safe_load(f)

        # Check if this is a Hydra-style config with defaults
        if "defaults" in main_cfg:
            # Process imports manually
            merged_cfg = _merge_hydra_configs(main_cfg, config_dir)
        else:
            # Regular YAML config
            merged_cfg = main_cfg

        # Convert dict to OmegaConf and use structured configs for conversion
        cfg_dict = OmegaConf.create(merged_cfg)
        # Merge with structured config schema to get proper defaults
        structured_cfg = OmegaConf.structured(ClearedConfig)
        merged_cfg = OmegaConf.merge(structured_cfg, cfg_dict)
        return instantiate(merged_cfg, _convert_="object")


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
    # This function is kept for backward compatibility
    # The actual initialization is now done in _initialize_config_store()
    _initialize_config_store()


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
