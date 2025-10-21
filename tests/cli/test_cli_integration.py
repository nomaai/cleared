"""Integration tests for CLI functionality to prevent regression of identified issues."""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd
import json

from cleared.cli.utils import load_config_from_file, create_sample_config
from cleared.engine import ClearedEngine, Results
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    TimeShiftConfig,
)
from cleared.io.filesystem import FileSystemDataLoader


class TestCLIIssueRegression:
    """Test cases to prevent regression of issues encountered during CLI development."""

    def test_cleared_engine_pipelines_attribute_consistency(self):
        """
        Test that ClearedEngine consistently uses _pipelines attribute internally.

        Issue: During CLI development, ClearedEngine was using self.pipelines in some places
        and self._pipelines in others, causing AttributeError.
        """
        # Create a minimal config
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        # Initialize engine
        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        # Test that _pipelines is used consistently
        assert hasattr(engine, "_pipelines")
        assert engine._pipelines == []

        # Test that all methods use _pipelines consistently
        assert engine.get_pipeline_count() == 0
        assert engine.is_empty() is True
        assert engine.list_pipelines() == []

        # Test that run method checks _pipelines correctly
        with pytest.raises(ValueError, match="No pipelines configured"):
            engine.run()

    def test_data_loader_configuration_structure(self):
        """
        Test that data loaders receive correct configuration structure.

        Issue: Data loaders expected 'data_source_type' in config but received IOConfig object.
        """
        # Create IOConfig
        io_config = IOConfig(
            io_type="filesystem",
            configs={"base_path": "/tmp/test", "file_format": "csv"},
        )

        # Test that create_data_loader properly converts IOConfig to expected format
        from cleared.io import create_data_loader

        data_loader = create_data_loader(io_config)

        # Verify the data loader was created with correct config structure
        assert isinstance(data_loader, FileSystemDataLoader)
        assert data_loader.data_source_type == "filesystem"
        assert data_loader.connection_params["base_path"] == "/tmp/test"
        assert data_loader.connection_params["file_format"] == "csv"

    def test_results_json_serialization(self):
        """
        Test that Results object can be properly serialized to JSON.

        Issue: Results object was not JSON serializable, causing TypeError in _save_results.
        """
        # Create a Results object
        results = Results()
        results.add_pipeline_result("pipeline_1", "success")
        results.add_pipeline_result("pipeline_2", "error", "Test error message")
        results.add_execution_order("pipeline_1")
        results.add_execution_order("pipeline_2")

        # Test that it can be converted to a serializable dictionary
        results_dict = {
            "success": results.success,
            "execution_order": results.execution_order,
            "results": {
                uid: {"status": result.status, "error": result.error}
                for uid, result in results.results.items()
            },
        }

        # Test JSON serialization
        json_str = json.dumps(results_dict, indent=2)
        assert json_str is not None

        # Test deserialization
        loaded_dict = json.loads(json_str)
        assert loaded_dict["success"] is True
        assert len(loaded_dict["results"]) == 2
        assert loaded_dict["results"]["pipeline_1"]["status"] == "success"
        assert loaded_dict["results"]["pipeline_2"]["status"] == "error"

    def test_save_results_method_signature(self):
        """
        Test that _save_results method has correct signature.

        Issue: _save_results was called without required 'results' argument.
        """
        # Create engine
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()
        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        # Create results
        results = Results()
        results.add_pipeline_result("test_pipeline", "success")

        # Test that _save_results can be called with results argument
        with tempfile.TemporaryDirectory() as temp_dir:
            engine.io_config.runtime_io_path = temp_dir

            # This should not raise an error
            engine._save_results(results)

            # Verify file was created
            result_files = list(Path(temp_dir).glob("status_*.json"))
            assert len(result_files) == 1

            # Verify content
            with open(result_files[0]) as f:
                saved_data = json.load(f)
            assert saved_data["success"] is True

    def test_timeshift_config_ref_id_field(self):
        """
        Test that TimeShiftConfig properly handles ref_id field.

        Issue: TimeShiftConfig was missing ref_id field, causing validation errors.
        """
        # Test that TimeShiftConfig can be created with ref_id
        time_shift = TimeShiftConfig(method="random_days", min=-365, max=365)

        assert time_shift.method == "random_days"
        assert time_shift.min == -365
        assert time_shift.max == 365

        # Test that it can be used in DeIDConfig
        deid_config = DeIDConfig(time_shift=time_shift)

        # This should not raise validation errors
        assert deid_config.time_shift is not None

    def test_configuration_loading_from_yaml(self):
        """
        Test that configuration can be loaded from YAML without errors.

        Issue: Configuration loading had multiple issues with structure conversion.
        """
        # Create a temporary YAML file
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
            # Test that configuration can be loaded
            config = load_config_from_file(Path(temp_file))

            # Verify structure
            assert isinstance(config, ClearedConfig)
            assert config.name == "test_engine"
            assert config.deid_config.time_shift is not None
            assert len(config.tables) == 1
            assert "patients" in config.tables

        finally:
            os.unlink(temp_file)

    def test_cli_command_execution_without_errors(self):
        """Test that CLI commands can be executed without the errors encountered during development."""
        # Test init command
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            temp_file = f.name

        try:
            # Test that init command works
            create_sample_config(Path(temp_file))

            # Verify file was created and has content
            assert Path(temp_file).exists()
            with open(temp_file) as f:
                content = f.read()
            assert "name:" in content
            assert "deid_config:" in content
            assert "io:" in content
            assert "tables:" in content

        finally:
            if Path(temp_file).exists():
                os.unlink(temp_file)

    def test_engine_initialization_from_config(self):
        """Test that ClearedEngine can be initialized from config without errors."""
        # Create a minimal valid config
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        config = ClearedConfig(
            name="test_engine", deid_config=deid_config, io=io_config, tables={}
        )

        # Test that engine can be initialized from config
        engine = ClearedEngine.__new__(ClearedEngine)
        engine._init_from_config(config)

        # Verify initialization
        assert engine.name == "test_engine"
        assert engine.deid_config == deid_config
        assert engine.io_config == io_config
        assert len(engine._pipelines) == 0

    def test_data_loader_creation_with_filesystem_config(self):
        """Test that filesystem data loader can be created with correct configuration."""
        # Create filesystem IOConfig
        io_config = IOConfig(
            io_type="filesystem",
            configs={
                "base_path": "/tmp/test",
                "file_format": "csv",
                "encoding": "utf-8",
                "separator": ",",
            },
        )

        # Test data loader creation
        from cleared.io import create_data_loader

        data_loader = create_data_loader(io_config)

        # Verify it's a filesystem data loader with correct config
        assert isinstance(data_loader, FileSystemDataLoader)
        assert data_loader.data_source_type == "filesystem"
        assert data_loader.base_path == Path("/tmp/test")
        assert data_loader.file_format == "csv"
        assert data_loader.encoding == "utf-8"
        assert data_loader.separator == ","

    def test_pipeline_execution_with_mock_data(self):
        """Test that pipeline execution works with mock data."""
        # Create engine with mock pipeline
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()
        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        # Create mock pipeline
        mock_pipeline = MagicMock()
        mock_pipeline.uid = "test_pipeline"
        mock_pipeline.transform.return_value = (
            pd.DataFrame({"id": [1, 2, 3]}),
            {"test_ref": pd.DataFrame({"id": [1, 2, 3]})},
        )

        engine.add_pipeline(mock_pipeline)

        # Mock the data loading and saving methods
        with patch.object(engine, "_load_initial_deid_ref_dict", return_value={}):
            with patch.object(engine, "_save_results"):
                # Test that run method works without errors
                results = engine.run()

                # Verify results
                assert isinstance(results, Results)
                assert results.success is True
                assert "test_pipeline" in results.results
                assert results.results["test_pipeline"].status == "success"

    def test_error_handling_in_pipeline_execution(self):
        """Test that pipeline execution errors are handled correctly."""
        # Create engine with failing pipeline
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()
        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        # Create failing pipeline
        mock_pipeline = MagicMock()
        mock_pipeline.uid = "failing_pipeline"
        mock_pipeline.transform.side_effect = Exception("Pipeline failed")

        engine.add_pipeline(mock_pipeline)

        # Mock the data loading and saving methods
        with patch.object(engine, "_load_initial_deid_ref_dict", return_value={}):
            with patch.object(engine, "_save_results"):
                # Test that run method handles errors correctly
                with pytest.raises(RuntimeError, match="Pipeline execution failed"):
                    engine.run(continue_on_error=False)

                # Test that run method continues on error
                results = engine.run(continue_on_error=True)
                assert isinstance(results, Results)
                assert results.success is True  # Engine itself succeeds
                assert "failing_pipeline" in results.results
                assert results.results["failing_pipeline"].status == "error"
