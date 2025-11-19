"""Unit tests for CLI utility functions to prevent regression of identified issues."""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch
import yaml

from cleared.cli.utils import (
    load_config_from_file,
    create_sample_config,
    validate_paths,
    create_missing_directories,
)
from cleared.config.structure import (
    ClearedConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
)


class TestConfigLoading:
    """Test configuration loading functionality."""

    def test_load_config_from_yaml_file(self):
        """Test loading configuration from YAML file."""
        yaml_content = """
name: "test_engine"
deid_config:
  time_shift:
    method: "random_days"
    min: -365
    max: 365
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
tables:
  patients:
    name: "patients"
    depends_on: []
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        depends_on: []
        configs:
          idconfig:
            name: "patient_id"
            uid: "patient_id"
            description: "Patient identifier"
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_file = f.name

        try:
            config = load_config_from_file(Path(temp_file))

            # Verify basic structure
            assert isinstance(config, ClearedConfig)
            assert config.name == "test_engine"

            # Verify deid_config
            assert config.deid_config.time_shift is not None

            # Verify io_config
            assert config.io.data.input_config.io_type == "filesystem"
            assert config.io.data.input_config.configs["base_path"] == "/tmp/input"
            assert config.io.runtime_io_path == "/tmp/runtime"

            # Verify tables
            assert len(config.tables) == 1
            assert "patients" in config.tables
            assert len(config.tables["patients"].transformers) == 1

        finally:
            os.unlink(temp_file)

    def test_load_config_with_minimal_structure(self):
        """Test loading configuration with minimal required fields."""
        yaml_content = """
name: "minimal_engine"
deid_config: {}
io:
  data:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "/tmp/input"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "/tmp/output"
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

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_file = f.name

        try:
            config = load_config_from_file(Path(temp_file))

            assert isinstance(config, ClearedConfig)
            assert config.name == "minimal_engine"
            assert config.deid_config.time_shift is None
            assert config.tables == {}

        finally:
            os.unlink(temp_file)

    def test_hydra_to_cleared_config_conversion(self):
        """Test conversion from Hydra config to ClearedConfig using structured configs."""
        from omegaconf import OmegaConf
        from hydra.utils import instantiate
        from cleared.config.structure import ClearedConfig

        hydra_config = {
            "name": "test_engine",
            "deid_config": {
                "time_shift": {"method": "random_days", "min": -365, "max": 365}
            },
            "io": {
                "data": {
                    "input_config": {
                        "io_type": "filesystem",
                        "configs": {"base_path": "/tmp/input", "file_format": "csv"},
                    },
                    "output_config": {
                        "io_type": "filesystem",
                        "configs": {"base_path": "/tmp/output", "file_format": "csv"},
                    },
                },
                "deid_ref": {
                    "input_config": {
                        "io_type": "filesystem",
                        "configs": {"base_path": "/tmp/deid_ref_input"},
                    },
                    "output_config": {
                        "io_type": "filesystem",
                        "configs": {"base_path": "/tmp/deid_ref_output"},
                    },
                },
                "runtime_io_path": "/tmp/runtime",
            },
            "tables": {
                "patients": {
                    "name": "patients",
                    "depends_on": [],
                    "transformers": [
                        {
                            "method": "IDDeidentifier",
                            "uid": "patient_id_transformer",
                            "depends_on": [],
                            "configs": {
                                "idconfig": {
                                    "name": "patient_id",
                                    "uid": "patient_id",
                                    "description": "Patient identifier",
                                }
                            },
                        }
                    ],
                }
            },
        }

        # Use structured configs for conversion (same approach as load_config_from_file)
        cfg_dict = OmegaConf.create(hydra_config)
        structured_cfg = OmegaConf.structured(ClearedConfig)
        merged_cfg = OmegaConf.merge(structured_cfg, cfg_dict)
        config = instantiate(merged_cfg, _convert_="object")

        # Verify conversion
        assert isinstance(config, ClearedConfig)
        assert config.name == "test_engine"
        assert config.deid_config.time_shift is not None
        assert config.io.data.input_config.io_type == "filesystem"
        assert len(config.tables) == 1

    def test_hydra_to_cleared_config_with_none_values(self):
        """Test conversion with None values in optional fields using structured configs."""
        from omegaconf import OmegaConf
        from hydra.utils import instantiate
        from cleared.config.structure import ClearedConfig

        hydra_config = {
            "name": "test_engine",
            "deid_config": {"time_shift": None},  # time_shift is Optional
            "io": {
                "data": {
                    "input_config": {
                        "io_type": "filesystem",
                        "configs": {"base_path": "/tmp/input"},
                    },
                    "output_config": {
                        "io_type": "filesystem",
                        "configs": {"base_path": "/tmp/output"},
                    },
                },
                "deid_ref": {
                    # input_config is required, so we provide it
                    "input_config": {
                        "io_type": "filesystem",
                        "configs": {"base_path": "/tmp/deid_ref_input"},
                    },
                    "output_config": {
                        "io_type": "filesystem",
                        "configs": {"base_path": "/tmp/deid_ref_output"},
                    },
                },
                "runtime_io_path": "/tmp/runtime",
            },
            "tables": {},
        }

        # Use structured configs for conversion (same approach as load_config_from_file)
        cfg_dict = OmegaConf.create(hydra_config)
        structured_cfg = OmegaConf.structured(ClearedConfig)
        merged_cfg = OmegaConf.merge(structured_cfg, cfg_dict)
        config = instantiate(merged_cfg, _convert_="object")

        # Verify conversion handles None values in optional fields
        assert isinstance(config, ClearedConfig)
        assert config.deid_config.time_shift is None
        assert config.tables == {}


class TestSampleConfigCreation:
    """Test sample configuration creation functionality."""

    def test_create_sample_config(self):
        """Test creating sample configuration file."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            temp_file = f.name

        try:
            create_sample_config(Path(temp_file))

            # Verify file was created
            assert Path(temp_file).exists()

            # Verify content
            with open(temp_file) as f:
                content = f.read()

            # Check for key sections
            assert "name:" in content
            assert "deid_config:" in content
            assert "time_shift:" in content
            assert "io:" in content
            assert "data:" in content
            assert "deid_ref:" in content
            assert "tables:" in content
            # Note: transformers may not be present in minimal config fallback

            # Verify it's valid YAML
            with open(temp_file) as f:
                yaml.safe_load(f)

        finally:
            if Path(temp_file).exists():
                os.unlink(temp_file)

    def test_create_sample_config_overwrite_existing(self):
        """Test creating sample config when file already exists."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            temp_file = f.name
            f.write("existing content")

        try:
            # Should not raise error when overwriting
            create_sample_config(Path(temp_file))

            # Verify content was overwritten
            with open(temp_file) as f:
                content = f.read()
            assert "name:" in content
            assert "existing content" not in content

        finally:
            if Path(temp_file).exists():
                os.unlink(temp_file)


class TestPathValidation:
    """Test path validation functionality."""

    def test_validate_paths_all_exist(self):
        """Test path validation when all paths exist."""
        config = ClearedConfig(name="test", io=ClearedIOConfig.default())

        with patch("pathlib.Path.exists", return_value=True):
            path_status = validate_paths(config)

            # All paths should exist
            for _, exists in path_status.items():
                assert exists is True

    def test_validate_paths_some_missing(self):
        """Test path validation when some paths are missing."""
        config = ClearedConfig(
            name="test",
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/nonexistent/input"},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/nonexistent/output"},
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/nonexistent/deid_input"},
                    ),
                    output_config=IOConfig(
                        io_type="filesystem",
                        configs={"base_path": "/nonexistent/deid_output"},
                    ),
                ),
                runtime_io_path="/nonexistent/runtime",
            ),
        )

        # Test with paths that don't exist
        path_status = validate_paths(config)

        # All paths should be missing
        missing_paths = [name for name, exists in path_status.items() if not exists]
        assert len(missing_paths) > 0
        assert len(missing_paths) == len(path_status)  # All paths should be missing

    def test_validate_paths_with_none_config(self):
        """Test path validation with None configuration values."""
        config = ClearedConfig(
            name="test",
            io=ClearedIOConfig(
                data=PairedIOConfig(
                    input_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/input"}
                    ),
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/output"}
                    ),
                ),
                deid_ref=PairedIOConfig(
                    input_config=None,  # None input config
                    output_config=IOConfig(
                        io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                    ),
                ),
                runtime_io_path="/tmp/runtime",
            ),
        )

        with patch("pathlib.Path.exists", return_value=True):
            path_status = validate_paths(config)

            # Should not include deid_ref_input since it's None
            assert "deid_ref_input" not in path_status
            assert "data_input" in path_status
            assert "data_output" in path_status
            assert "deid_ref_output" in path_status
            assert "runtime" in path_status


class TestDirectoryCreation:
    """Test directory creation functionality."""

    def test_create_missing_directories(self):
        """Test creating missing directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create config with temp directory paths
            config = ClearedConfig(
                name="test",
                io=ClearedIOConfig(
                    data=PairedIOConfig(
                        input_config=IOConfig(
                            io_type="filesystem",
                            configs={"base_path": f"{temp_dir}/input"},
                        ),
                        output_config=IOConfig(
                            io_type="filesystem",
                            configs={"base_path": f"{temp_dir}/output"},
                        ),
                    ),
                    deid_ref=PairedIOConfig(
                        input_config=IOConfig(
                            io_type="filesystem",
                            configs={"base_path": f"{temp_dir}/deid_input"},
                        ),
                        output_config=IOConfig(
                            io_type="filesystem",
                            configs={"base_path": f"{temp_dir}/deid_output"},
                        ),
                    ),
                    runtime_io_path=f"{temp_dir}/runtime",
                ),
            )

            # Test that directories are created
            create_missing_directories(config)

            # Verify directories were created
            assert Path(f"{temp_dir}/input").exists()
            assert Path(f"{temp_dir}/output").exists()
            assert Path(f"{temp_dir}/deid_input").exists()
            assert Path(f"{temp_dir}/deid_output").exists()
            assert Path(f"{temp_dir}/runtime").exists()

    def test_create_missing_directories_with_none_config(self):
        """Test creating directories with None configuration values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = ClearedConfig(
                name="test",
                io=ClearedIOConfig(
                    data=PairedIOConfig(
                        input_config=IOConfig(
                            io_type="filesystem",
                            configs={"base_path": f"{temp_dir}/input"},
                        ),
                        output_config=IOConfig(
                            io_type="filesystem",
                            configs={"base_path": f"{temp_dir}/output"},
                        ),
                    ),
                    deid_ref=PairedIOConfig(
                        input_config=None,  # None input config
                        output_config=IOConfig(
                            io_type="filesystem",
                            configs={"base_path": f"{temp_dir}/deid_output"},
                        ),
                    ),
                    runtime_io_path=f"{temp_dir}/runtime",
                ),
            )

            # Test that directories are created
            create_missing_directories(config)

            # Should not create deid_ref_input since it's None
            assert not Path(f"{temp_dir}/deid_ref_input").exists()
            assert Path(f"{temp_dir}/input").exists()
            assert Path(f"{temp_dir}/output").exists()
            assert Path(f"{temp_dir}/deid_output").exists()
            assert Path(f"{temp_dir}/runtime").exists()


class TestErrorHandling:
    """Test error handling in CLI utilities."""

    def test_load_config_file_not_found(self):
        """Test loading configuration from non-existent file."""
        with pytest.raises(FileNotFoundError):
            load_config_from_file(Path("nonexistent.yaml"))

    def test_load_config_invalid_yaml(self):
        """Test loading configuration from invalid YAML file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [")
            temp_file = f.name

        try:
            with pytest.raises(yaml.YAMLError):
                load_config_from_file(Path(temp_file))
        finally:
            os.unlink(temp_file)

    def test_load_config_missing_required_fields(self):
        """Test loading configuration with missing required fields."""
        yaml_content = """
name: "test_engine"
# Missing deid_config and io
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_file = f.name

        try:
            # Should not raise error, but should use defaults
            config = load_config_from_file(Path(temp_file))
            assert config.name == "test_engine"
            assert config.deid_config.time_shift is None
            assert config.tables == {}
        finally:
            os.unlink(temp_file)
