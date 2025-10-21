"""Unit tests for engine issues encountered during CLI development."""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd

from cleared.engine import ClearedEngine, Results
from cleared.config.structure import (
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
)


class TestEngineAttributeConsistency:
    """Test that ClearedEngine uses consistent attribute names."""

    def test_engine_uses_pipelines_attribute_consistently(self):
        """
        Test that ClearedEngine consistently uses _pipelines attribute.

        Issue: During CLI development, some methods used self.pipelines while others used self._pipelines.
        """
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        # Test that _pipelines is the correct attribute
        assert hasattr(engine, "_pipelines")
        assert not hasattr(engine, "pipelines")

        # Test all methods use _pipelines consistently
        assert engine._pipelines == []
        assert engine.get_pipeline_count() == 0
        assert engine.is_empty() is True
        assert engine.list_pipelines() == []

    def test_engine_run_method_checks_pipelines_correctly(self):
        """Test that run method checks _pipelines correctly."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        # Test with empty pipelines
        with pytest.raises(ValueError, match="No pipelines configured"):
            engine.run()

        # Test with None pipelines
        engine._pipelines = None
        with pytest.raises(ValueError, match="No pipelines configured"):
            engine.run()

    def test_engine_pipeline_management_uses_pipelines_consistently(self):
        """Test that pipeline management methods use _pipelines consistently."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        # Create mock pipeline
        mock_pipeline = MagicMock()
        mock_pipeline.uid = "test_pipeline"

        # Test add_pipeline
        engine.add_pipeline(mock_pipeline)
        assert len(engine._pipelines) == 1
        assert engine._pipelines[0] == mock_pipeline

        # Test get_pipeline_count
        assert engine.get_pipeline_count() == 1

        # Test is_empty
        assert engine.is_empty() is False

        # Test list_pipelines
        assert engine.list_pipelines() == ["test_pipeline"]

        # Test get_pipeline
        retrieved_pipeline = engine.get_pipeline("test_pipeline")
        assert retrieved_pipeline == mock_pipeline

        # Test remove_pipeline
        removed = engine.remove_pipeline("test_pipeline")
        assert removed is True
        assert len(engine._pipelines) == 0
        assert engine.is_empty() is True

    def test_engine_special_methods_use_pipelines_consistently(self):
        """Test that special methods use _pipelines consistently."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        # Test __len__
        assert len(engine) == 0

        # Test __bool__
        assert bool(engine) is False

        # Add pipeline
        mock_pipeline = MagicMock()
        mock_pipeline.uid = "test_pipeline"
        engine.add_pipeline(mock_pipeline)

        # Test __len__ with pipeline
        assert len(engine) == 1

        # Test __bool__ with pipeline
        assert bool(engine) is True

    def test_engine_repr_uses_pipelines_consistently(self):
        """Test that __repr__ uses _pipelines consistently."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        # Test repr with no pipelines
        repr_str = repr(engine)
        assert "pipelines=0" in repr_str

        # Add pipeline
        mock_pipeline = MagicMock()
        mock_pipeline.uid = "test_pipeline"
        engine.add_pipeline(mock_pipeline)

        # Test repr with pipeline
        repr_str = repr(engine)
        assert "pipelines=1" in repr_str


class TestResultsSerialization:
    """Test Results object serialization issues."""

    def test_results_json_serialization(self):
        """
        Test that Results object can be serialized to JSON.

        Issue: Results object was not JSON serializable, causing TypeError in _save_results.
        """
        results = Results()
        results.add_pipeline_result("pipeline_1", "success")
        results.add_pipeline_result("pipeline_2", "error", "Test error message")
        results.add_execution_order("pipeline_1")
        results.add_execution_order("pipeline_2")

        # Test manual serialization (as done in _save_results)
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
        assert loaded_dict["results"]["pipeline_2"]["error"] == "Test error message"

    def test_results_with_none_error(self):
        """Test Results serialization with None error values."""
        results = Results()
        results.add_pipeline_result("pipeline_1", "success")
        results.add_pipeline_result("pipeline_2", "skipped")

        # Test serialization with None errors
        results_dict = {
            "success": results.success,
            "execution_order": results.execution_order,
            "results": {
                uid: {"status": result.status, "error": result.error}
                for uid, result in results.results.items()
            },
        }

        json_str = json.dumps(results_dict, indent=2)
        loaded_dict = json.loads(json_str)

        assert loaded_dict["results"]["pipeline_1"]["error"] is None
        assert loaded_dict["results"]["pipeline_2"]["error"] is None

    def test_results_with_empty_string_error(self):
        """Test Results serialization with empty string error values."""
        results = Results()
        results.add_pipeline_result("pipeline_1", "error", "")

        results_dict = {
            "success": results.success,
            "execution_order": results.execution_order,
            "results": {
                uid: {"status": result.status, "error": result.error}
                for uid, result in results.results.items()
            },
        }

        json_str = json.dumps(results_dict, indent=2)
        loaded_dict = json.loads(json_str)

        assert loaded_dict["results"]["pipeline_1"]["error"] == ""

    def test_save_results_method_signature(self):
        """
        Test that _save_results method has correct signature.

        Issue: _save_results was called without required 'results' argument.
        """
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
            assert "test_pipeline" in saved_data["results"]

    def test_save_results_with_complex_results(self):
        """Test saving complex results with multiple pipelines and errors."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        # Create complex results
        results = Results()
        results.add_pipeline_result("pipeline_1", "success")
        results.add_pipeline_result("pipeline_2", "error", "Connection failed")
        results.add_pipeline_result("pipeline_3", "skipped")
        results.add_execution_order("pipeline_1")
        results.add_execution_order("pipeline_2")
        results.add_execution_order("pipeline_3")
        results.set_success(False)

        with tempfile.TemporaryDirectory() as temp_dir:
            engine.io_config.runtime_io_path = temp_dir

            engine._save_results(results)

            # Verify file was created
            result_files = list(Path(temp_dir).glob("status_*.json"))
            assert len(result_files) == 1

            # Verify content
            with open(result_files[0]) as f:
                saved_data = json.load(f)

            assert saved_data["success"] is False
            assert len(saved_data["results"]) == 3
            assert saved_data["results"]["pipeline_1"]["status"] == "success"
            assert saved_data["results"]["pipeline_2"]["status"] == "error"
            assert saved_data["results"]["pipeline_2"]["error"] == "Connection failed"
            assert saved_data["results"]["pipeline_3"]["status"] == "skipped"
            assert saved_data["execution_order"] == [
                "pipeline_1",
                "pipeline_2",
                "pipeline_3",
            ]


class TestEngineInitialization:
    """Test engine initialization issues."""

    def test_engine_initialization_with_none_pipelines(self):
        """Test engine initialization with None pipelines."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine",
            pipelines=None,
            deid_config=deid_config,
            io_config=io_config,
        )

        # Should initialize with empty list
        assert engine._pipelines == []

    def test_engine_initialization_with_empty_pipelines(self):
        """Test engine initialization with empty pipelines list."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine",
            pipelines=[],
            deid_config=deid_config,
            io_config=io_config,
        )

        # Should initialize with empty list
        assert engine._pipelines == []

    def test_engine_initialization_with_multiple_pipelines(self):
        """Test engine initialization with multiple pipelines."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        mock_pipeline1 = MagicMock()
        mock_pipeline1.uid = "pipeline_1"
        mock_pipeline2 = MagicMock()
        mock_pipeline2.uid = "pipeline_2"

        engine = ClearedEngine(
            name="test_engine",
            pipelines=[mock_pipeline1, mock_pipeline2],
            deid_config=deid_config,
            io_config=io_config,
        )

        # Should initialize with provided pipelines
        assert len(engine._pipelines) == 2
        assert engine._pipelines[0] == mock_pipeline1
        assert engine._pipelines[1] == mock_pipeline2

    def test_engine_initialization_with_none_registry(self):
        """Test engine initialization with None registry."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine",
            registry=None,
            deid_config=deid_config,
            io_config=io_config,
        )

        # Should create default registry
        assert engine._registry is not None
        assert hasattr(engine._registry, "use_defaults") or hasattr(
            engine._registry, "register"
        )

    def test_engine_initialization_with_none_io_config(self):
        """Test engine initialization with None io_config."""
        deid_config = DeIDConfig()

        with pytest.raises(ValueError, match="IO Config is required"):
            ClearedEngine(name="test_engine", deid_config=deid_config, io_config=None)

    def test_engine_initialization_with_invalid_io_config(self):
        """Test engine initialization with invalid io_config."""
        deid_config = DeIDConfig()

        # Test with missing deid_ref
        invalid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=None,
            runtime_io_path="/tmp/runtime",
        )

        with pytest.raises(
            ValueError,
            match="De-identification IO config must contain at least outout io configurations",
        ):
            ClearedEngine(
                name="test_engine", deid_config=deid_config, io_config=invalid_io_config
            )


class TestEngineRunMethod:
    """Test engine run method issues."""

    def test_engine_run_with_no_pipelines(self):
        """Test engine run with no pipelines configured."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )

        with pytest.raises(ValueError, match="No pipelines configured"):
            engine.run()

    def test_engine_run_with_none_pipelines(self):
        """Test engine run with None pipelines."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )
        engine._pipelines = None

        with pytest.raises(ValueError, match="No pipelines configured"):
            engine.run()

    def test_engine_run_with_empty_pipelines_list(self):
        """Test engine run with empty pipelines list."""
        deid_config = DeIDConfig()
        io_config = ClearedIOConfig.default()

        engine = ClearedEngine(
            name="test_engine", deid_config=deid_config, io_config=io_config
        )
        engine._pipelines = []

        with pytest.raises(ValueError, match="No pipelines configured"):
            engine.run()

    def test_engine_run_with_successful_pipeline(self):
        """Test engine run with successful pipeline."""
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
                results = engine.run()

                # Verify results
                assert isinstance(results, Results)
                assert results.success is True
                assert "test_pipeline" in results.results
                assert results.results["test_pipeline"].status == "success"

    def test_engine_run_with_failing_pipeline_continue_on_error(self):
        """Test engine run with failing pipeline and continue_on_error=True."""
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
                results = engine.run(continue_on_error=True)

                # Verify results
                assert isinstance(results, Results)
                assert results.success is True  # Engine itself succeeds
                assert "failing_pipeline" in results.results
                assert results.results["failing_pipeline"].status == "error"

    def test_engine_run_with_failing_pipeline_no_continue(self):
        """Test engine run with failing pipeline and continue_on_error=False."""
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
                with pytest.raises(RuntimeError, match="Pipeline execution failed"):
                    engine.run(continue_on_error=False)
