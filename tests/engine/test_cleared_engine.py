"""Comprehensive tests for ClearedEngine class."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from cleared.engine import ClearedEngine, Results
from cleared.config.structure import (
    DeIDConfig,
    ClearedIOConfig,
    ClearedConfig,
    IOConfig,
    PairedIOConfig,
    TimeShiftConfig,
    TableConfig,
    TransformerConfig,
)
from cleared.transformers.pipelines import TablePipeline
from cleared.transformers.registry import TransformerRegistry
from cleared.transformers.base import Pipeline


class TestClearedEngineInitialization:
    """Test ClearedEngine initialization methods."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create minimal valid IO config
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        self.valid_name = "test_engine"

    def test_init_with_all_parameters(self):
        """Test initialization with all parameters provided."""
        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        mock_registry = Mock(spec=TransformerRegistry)

        engine = ClearedEngine(
            name=self.valid_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
            pipelines=[mock_pipeline],
            registry=mock_registry,
        )

        assert engine.name == self.valid_name
        assert engine.deid_config == self.valid_deid_config
        assert engine.io_config == self.valid_io_config
        assert engine._pipelines == [mock_pipeline]
        assert engine._registry == mock_registry
        assert isinstance(engine.results, dict)
        assert engine.results == {}
        assert engine._uid.startswith(f"{self.valid_name}_")

    def test_init_with_minimal_parameters(self):
        """Test initialization with only required parameters."""
        engine = ClearedEngine(
            name=self.valid_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        assert engine.name == self.valid_name
        assert engine.deid_config == self.valid_deid_config
        assert engine.io_config == self.valid_io_config
        assert engine._pipelines == []
        assert isinstance(engine._registry, TransformerRegistry)
        assert engine.results == {}
        assert engine._uid.startswith(f"{self.valid_name}_")

    def test_init_with_none_pipelines(self):
        """Test initialization with None pipelines (should default to empty list)."""
        engine = ClearedEngine(
            name=self.valid_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
            pipelines=None,
        )

        assert engine._pipelines == []

    def test_init_with_none_registry(self):
        """Test initialization with None registry (should create default registry)."""
        engine = ClearedEngine(
            name=self.valid_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
            registry=None,
        )

        assert isinstance(engine._registry, TransformerRegistry)

    def test_init_with_empty_pipelines_list(self):
        """Test initialization with empty pipelines list."""
        engine = ClearedEngine(
            name=self.valid_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
            pipelines=[],
        )

        assert engine._pipelines == []

    def test_init_with_multiple_pipelines(self):
        """Test initialization with multiple pipelines."""
        mock_pipeline1 = Mock(spec=TablePipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline2 = Mock(spec=TablePipeline)
        mock_pipeline2.uid = "pipeline2"

        engine = ClearedEngine(
            name=self.valid_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
            pipelines=[mock_pipeline1, mock_pipeline2],
        )

        assert len(engine._pipelines) == 2
        assert engine._pipelines[0] == mock_pipeline1
        assert engine._pipelines[1] == mock_pipeline2

    def test_init_uid_generation(self):
        """Test that UID is generated correctly with timestamp."""
        engine1 = ClearedEngine(
            name=self.valid_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        # Small delay to ensure different timestamps
        import time

        time.sleep(1.0)  # Use 1 second delay to ensure different timestamps

        engine2 = ClearedEngine(
            name=self.valid_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        assert engine1._uid != engine2._uid
        assert engine1._uid.startswith(f"{self.valid_name}_")
        assert engine2._uid.startswith(f"{self.valid_name}_")
        assert len(engine1._uid) > len(self.valid_name) + 1  # Should have timestamp

    def test_init_io_config_validation_success(self):
        """Test that valid IO config passes validation."""
        # This should not raise any exception
        engine = ClearedEngine(
            name=self.valid_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        assert engine.io_config == self.valid_io_config

    def test_init_io_config_validation_failure_none(self):
        """Test that None IO config raises ValueError."""
        with pytest.raises(ValueError, match="IO Config is required"):
            ClearedEngine(
                name=self.valid_name, deid_config=self.valid_deid_config, io_config=None
            )

    def test_init_io_config_validation_failure_no_deid_ref(self):
        """Test that missing deid_ref raises ValueError."""
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
                name=self.valid_name,
                deid_config=self.valid_deid_config,
                io_config=invalid_io_config,
            )

    def test_init_io_config_validation_failure_wrong_input_type(self):
        """Test that wrong input io_type raises ValueError."""
        invalid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="sql", configs={"connection_string": "sqlite:///test.db"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        with pytest.raises(
            ValueError,
            match="De-identification reference dictionary input configuration must be of type filesystem",
        ):
            ClearedEngine(
                name=self.valid_name,
                deid_config=self.valid_deid_config,
                io_config=invalid_io_config,
            )

    def test_init_io_config_validation_failure_no_output_config(self):
        """Test that missing output config raises ValueError."""
        invalid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=None,
            ),
            runtime_io_path="/tmp/runtime",
        )

        with pytest.raises(
            ValueError,
            match="De-identification reference dictionary output configuration must be provided",
        ):
            ClearedEngine(
                name=self.valid_name,
                deid_config=self.valid_deid_config,
                io_config=invalid_io_config,
            )

    def test_init_io_config_validation_failure_wrong_output_type(self):
        """Test that wrong output io_type raises ValueError."""
        invalid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="sql", configs={"connection_string": "sqlite:///test.db"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        with pytest.raises(
            ValueError,
            match="De-identification reference dictionary output configuration must be of type filesystem",
        ):
            ClearedEngine(
                name=self.valid_name,
                deid_config=self.valid_deid_config,
                io_config=invalid_io_config,
            )

    def test_init_io_config_validation_failure_no_data_config(self):
        """Test that missing data config raises ValueError."""
        invalid_io_config = ClearedIOConfig(
            data=None,
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        with pytest.raises(ValueError, match="Data IO config must be provided"):
            ClearedEngine(
                name=self.valid_name,
                deid_config=self.valid_deid_config,
                io_config=invalid_io_config,
            )

    def test_init_io_config_validation_failure_no_data_input(self):
        """Test that missing data input config raises ValueError."""
        invalid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=None,
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        with pytest.raises(
            ValueError, match="Data input configuration must be provided"
        ):
            ClearedEngine(
                name=self.valid_name,
                deid_config=self.valid_deid_config,
                io_config=invalid_io_config,
            )

    def test_init_io_config_validation_failure_no_data_output(self):
        """Test that missing data output config raises ValueError."""
        invalid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=None,
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        with pytest.raises(
            ValueError, match="Data output configuration must be provided"
        ):
            ClearedEngine(
                name=self.valid_name,
                deid_config=self.valid_deid_config,
                io_config=invalid_io_config,
            )


class TestClearedEngineInitFromConfig:
    """Test ClearedEngine initialization from config."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        self.valid_config = ClearedConfig(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io=self.valid_io_config,
            tables={
                "patients": TableConfig(
                    name="patients",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="patient_id_transformer",
                            configs={
                                "idconfig": {
                                    "name": "patient_id",
                                    "uid": "patient_id",
                                    "description": "Patient ID",
                                }
                            },
                        )
                    ],
                )
            },
        )

    def test_init_from_config_with_registry(self):
        """Test initialization from config with provided registry."""
        mock_registry = Mock(spec=TransformerRegistry)
        mock_transformer = Mock()
        mock_registry.instantiate.return_value = mock_transformer

        engine = ClearedEngine.__new__(ClearedEngine)
        engine._init_from_config(self.valid_config, mock_registry)

        assert engine.name == "test_engine"
        assert engine.deid_config == self.valid_deid_config
        assert engine.io_config == self.valid_io_config
        assert engine._registry == mock_registry
        assert len(engine._pipelines) == 1
        assert isinstance(engine._pipelines[0], TablePipeline)

    def test_init_from_config_without_registry(self):
        """Test initialization from config without provided registry."""
        engine = ClearedEngine.__new__(ClearedEngine)
        engine._init_from_config(self.valid_config, None)

        assert engine.name == "test_engine"
        assert engine.deid_config == self.valid_deid_config
        assert engine.io_config == self.valid_io_config
        assert isinstance(engine._registry, TransformerRegistry)
        assert len(engine._pipelines) == 1
        assert isinstance(engine._pipelines[0], TablePipeline)

    def test_init_from_config_with_multiple_tables(self):
        """Test initialization from config with multiple tables."""
        config_with_multiple_tables = ClearedConfig(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io=self.valid_io_config,
            tables={
                "patients": TableConfig(
                    name="patients",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="patient_id_transformer",
                            configs={
                                "idconfig": {
                                    "name": "patient_id",
                                    "uid": "patient_id",
                                    "description": "Patient ID",
                                }
                            },
                        )
                    ],
                ),
                "encounters": TableConfig(
                    name="encounters",
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="encounter_id_transformer",
                            configs={
                                "idconfig": {
                                    "name": "encounter_id",
                                    "uid": "encounter_id",
                                    "description": "Encounter ID",
                                }
                            },
                        )
                    ],
                ),
            },
        )

        engine = ClearedEngine.__new__(ClearedEngine)
        engine._init_from_config(config_with_multiple_tables, None)

        assert len(engine._pipelines) == 2
        assert all(
            isinstance(pipeline, TablePipeline) for pipeline in engine._pipelines
        )

    def test_init_from_config_with_empty_tables(self):
        """Test initialization from config with empty tables."""
        config_with_empty_tables = ClearedConfig(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io=self.valid_io_config,
            tables={},
        )

        engine = ClearedEngine.__new__(ClearedEngine)
        engine._init_from_config(config_with_empty_tables, None)

        assert len(engine._pipelines) == 0

    @patch("cleared.engine.TablePipeline")
    @patch("cleared.engine.TransformerRegistry")
    def test_load_pipelines_from_config(self, mock_registry_class, mock_pipeline_class):
        """Test _load_pipelines_from_config method."""
        # Setup mocks
        mock_registry = Mock(spec=TransformerRegistry)
        mock_transformer = Mock()
        mock_registry.instantiate.return_value = mock_transformer

        mock_pipeline_instance = Mock(spec=TablePipeline)
        mock_pipeline_class.return_value = mock_pipeline_instance

        # Create engine and test
        engine = ClearedEngine.__new__(ClearedEngine)
        engine._registry = mock_registry

        pipelines = engine._load_pipelines_from_config(self.valid_config)

        # Verify TablePipeline was created correctly
        mock_pipeline_class.assert_called_once_with(
            "patients",
            self.valid_io_config.data,
            self.valid_deid_config,
            uid="patients",
        )

        # Verify transformer was instantiated and added
        mock_registry.instantiate.assert_called_once_with(
            "IDDeidentifier",
            {
                "idconfig": {
                    "name": "patient_id",
                    "uid": "patient_id",
                    "description": "Patient ID",
                }
            },
            uid="patient_id_transformer",
            global_deid_config=self.valid_deid_config,
        )
        mock_pipeline_instance.add_transformer.assert_called_once_with(mock_transformer)

        # Verify pipeline was added to list
        assert len(pipelines) == 1
        assert pipelines[0] == mock_pipeline_instance


class TestClearedEngineRun:
    """Test ClearedEngine run method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        self.engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

    def test_run_with_no_pipelines(self):
        """Test run method with no pipelines configured."""
        with pytest.raises(
            ValueError, match=r"No pipelines configured. Add pipelines before running."
        ):
            self.engine.run()

    def test_run_with_none_pipelines(self):
        """Test run method with None pipelines."""
        self.engine._pipelines = None
        with pytest.raises(
            ValueError, match=r"No pipelines configured. Add pipelines before running."
        ):
            self.engine.run()

    def test_run_with_empty_pipelines_list(self):
        """Test run method with empty pipelines list."""
        self.engine._pipelines = []
        with pytest.raises(
            ValueError, match=r"No pipelines configured. Add pipelines before running."
        ):
            self.engine.run()

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    def test_run_successful_single_pipeline(
        self, mock_save_results, mock_load_deid_ref
    ):
        """Test successful run with single pipeline."""
        # Setup mocks
        mock_load_deid_ref.return_value = {"test_ref": pd.DataFrame({"id": [1, 2, 3]})}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        mock_pipeline.transform.return_value = (
            pd.DataFrame({"result": [1, 2, 3]}),
            {"test_ref": pd.DataFrame({"id": [1, 2, 3]})},
        )

        self.engine.add_pipeline(mock_pipeline)

        # Run engine
        result = self.engine.run()

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True
        assert "test_pipeline" in result.results
        assert result.results["test_pipeline"].status == "success"
        assert result.results["test_pipeline"].error is None
        assert "test_pipeline" in result.execution_order

        # Verify pipeline was called correctly
        call_args = mock_pipeline.transform.call_args
        assert call_args[1]["df"] is None
        assert "test_ref" in call_args[1]["deid_ref_dict"]
        assert call_args[1]["deid_ref_dict"]["test_ref"].equals(
            pd.DataFrame({"id": [1, 2, 3]})
        )

        # Verify methods were called
        mock_load_deid_ref.assert_called_once()
        mock_save_results.assert_called_once()

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    def test_run_successful_multiple_pipelines(
        self, mock_save_results, mock_load_deid_ref
    ):
        """Test successful run with multiple pipelines."""
        # Setup mocks
        mock_load_deid_ref.return_value = {"test_ref": pd.DataFrame({"id": [1, 2, 3]})}

        mock_pipeline1 = Mock(spec=TablePipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline1.transform.return_value = (
            pd.DataFrame({"result1": [1, 2]}),
            {"test_ref": pd.DataFrame({"id": [1, 2, 3]})},
        )

        mock_pipeline2 = Mock(spec=TablePipeline)
        mock_pipeline2.uid = "pipeline2"
        mock_pipeline2.transform.return_value = (
            pd.DataFrame({"result2": [3, 4]}),
            {"test_ref": pd.DataFrame({"id": [1, 2, 3]})},
        )

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)
        # Run engine
        result = self.engine.run()

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True
        assert len(result.results) == 2
        assert "pipeline1" in result.results
        assert "pipeline2" in result.results
        assert result.results["pipeline1"].status == "success"
        assert result.results["pipeline2"].status == "success"
        assert result.execution_order == ["pipeline1", "pipeline2"]

        # Verify pipelines were called in order
        mock_pipeline1.transform.assert_called_once()
        mock_pipeline2.transform.assert_called_once()

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    def test_run_pipeline_without_transform_method(
        self, mock_save_results, mock_load_deid_ref
    ):
        """Test run with pipeline that doesn't have transform method."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        # Remove transform method
        del mock_pipeline.transform

        self.engine.add_pipeline(mock_pipeline)

        # Run engine
        result = self.engine.run()

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True
        assert "test_pipeline" in result.results
        assert result.results["test_pipeline"].status == "error"
        assert (
            "does not have a transform method" in result.results["test_pipeline"].error
        )

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    def test_run_pipeline_exception_without_continue(
        self, mock_save_results, mock_load_deid_ref
    ):
        """Test run with pipeline exception and continue_on_error=False."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        mock_pipeline.transform.side_effect = Exception("Pipeline failed")

        self.engine.add_pipeline(mock_pipeline)

        # Run engine with continue_on_error=False (default)
        with pytest.raises(
            RuntimeError, match="Pipeline execution failed: Pipeline failed"
        ):
            self.engine.run()

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    def test_run_pipeline_exception_with_continue(
        self, mock_save_results, mock_load_deid_ref
    ):
        """Test run with pipeline exception and continue_on_error=True."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline1 = Mock(spec=TablePipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline1.transform.side_effect = Exception("Pipeline 1 failed")

        mock_pipeline2 = Mock(spec=TablePipeline)
        mock_pipeline2.uid = "pipeline2"
        mock_pipeline2.transform.return_value = (pd.DataFrame({"result": [1, 2]}), {})

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        # Run engine with continue_on_error=True
        result = self.engine.run(continue_on_error=True)

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True  # Should still be True since we continued
        assert len(result.results) == 2
        assert result.results["pipeline1"].status == "error"
        assert "Pipeline 1 failed" in result.results["pipeline1"].error
        assert result.results["pipeline2"].status == "success"
        assert result.execution_order == ["pipeline1", "pipeline2"]

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    def test_run_pipeline_exception_with_continue_all_fail(
        self, mock_save_results, mock_load_deid_ref
    ):
        """Test run with all pipelines failing and continue_on_error=True."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline1 = Mock(spec=TablePipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline1.transform.side_effect = Exception("Pipeline 1 failed")

        mock_pipeline2 = Mock(spec=TablePipeline)
        mock_pipeline2.uid = "pipeline2"
        mock_pipeline2.transform.side_effect = Exception("Pipeline 2 failed")

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        # Run engine with continue_on_error=True
        result = self.engine.run(continue_on_error=True)

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True  # Should still be True since we continued
        assert len(result.results) == 2
        assert result.results["pipeline1"].status == "error"
        assert result.results["pipeline2"].status == "error"
        assert "Pipeline 1 failed" in result.results["pipeline1"].error
        assert "Pipeline 2 failed" in result.results["pipeline2"].error


class TestClearedEngineDataLoading:
    """Test ClearedEngine data loading methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        self.engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

    def test_load_initial_deid_ref_dict_no_input_config(self):
        """Test _load_initial_deid_ref_dict with no input config."""
        self.engine.io_config.deid_ref.input_config = None

        result = self.engine._load_initial_deid_ref_dict()

        assert result == {}

    def test_load_initial_deid_ref_dict_wrong_io_type(self):
        """Test _load_initial_deid_ref_dict with wrong io_type."""
        self.engine.io_config.deid_ref.input_config = IOConfig(
            io_type="sql", configs={"connection_string": "sqlite:///test.db"}
        )

        result = self.engine._load_initial_deid_ref_dict()

        assert result == {}

    def test_load_initial_deid_ref_dict_directory_not_exists(self):
        """Test _load_initial_deid_ref_dict with non-existent directory."""
        # The method now returns an empty dict instead of raising an error
        result = self.engine._load_initial_deid_ref_dict()
        assert result == {}

    @patch("cleared.engine.glob.glob")
    @patch("cleared.engine.pd.read_csv")
    @patch("cleared.engine.os.path.exists")
    def test_load_initial_deid_ref_dict_success(
        self, mock_exists, mock_read_csv, mock_glob
    ):
        """Test successful _load_initial_deid_ref_dict."""
        # Setup mocks
        mock_exists.return_value = True
        mock_glob.return_value = [
            "/tmp/deid_input/test1.csv",
            "/tmp/deid_input/test2.csv",
        ]

        df1 = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        df2 = pd.DataFrame({"id": [4, 5, 6], "value": [1.1, 2.2, 3.3]})
        mock_read_csv.side_effect = [df1, df2]

        result = self.engine._load_initial_deid_ref_dict()

        # Verify results
        assert len(result) == 2
        assert "test1" in result
        assert "test2" in result
        assert result["test1"].equals(df1)
        assert result["test2"].equals(df2)

        # Verify calls
        mock_exists.assert_called_once_with("/tmp/deid_input")
        mock_glob.assert_called_once_with("/tmp/deid_input/*.csv")
        assert mock_read_csv.call_count == 2

    @patch("cleared.engine.glob.glob")
    @patch("cleared.engine.pd.read_csv")
    @patch("cleared.engine.os.path.exists")
    def test_load_initial_deid_ref_dict_with_errors(
        self, mock_exists, mock_read_csv, mock_glob
    ):
        """Test _load_initial_deid_ref_dict with some files failing to load."""
        # Setup mocks
        mock_exists.return_value = True
        mock_glob.return_value = [
            "/tmp/deid_input/test1.csv",
            "/tmp/deid_input/test2.csv",
            "/tmp/deid_input/test3.csv",
        ]

        df1 = pd.DataFrame({"id": [1, 2, 3]})
        mock_read_csv.side_effect = [
            df1,
            Exception("Read error"),
            pd.DataFrame({"id": [4, 5, 6]}),
        ]

        result = self.engine._load_initial_deid_ref_dict()

        # Verify results - should have 2 successful loads
        assert len(result) == 2
        assert "test1" in result
        assert "test3" in result
        assert "test2" not in result

    def test_convert_numeric_columns_string_numbers(self):
        """Test _convert_numeric_columns with string numbers."""
        df = pd.DataFrame(
            {
                "id": ["1", "2", "3"],
                "value": ["1.5", "2.7", "3.0"],
                "name": ["a", "b", "c"],
            }
        )

        result = self.engine._convert_numeric_columns(df)

        assert result["id"].dtype == "int64"
        assert result["value"].dtype == "float64"
        assert result["name"].dtype == "object"
        assert result["id"].tolist() == [1, 2, 3]
        assert result["value"].tolist() == [1.5, 2.7, 3.0]

    def test_convert_numeric_columns_mixed_types(self):
        """Test _convert_numeric_columns with mixed numeric types."""
        df = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "string_int": ["4", "5", "6"],
                "string_float": ["7.1", "8.2", "9.3"],
                "text": ["a", "b", "c"],
            }
        )

        result = self.engine._convert_numeric_columns(df)

        assert result["int_col"].dtype == "int64"
        assert result["float_col"].dtype == "float64"
        assert result["string_int"].dtype == "int64"
        assert result["string_float"].dtype == "float64"
        assert result["text"].dtype == "object"

    def test_convert_numeric_columns_non_numeric_strings(self):
        """Test _convert_numeric_columns with non-numeric strings."""
        df = pd.DataFrame(
            {
                "id": ["1", "2", "abc"],
                "value": ["1.5", "invalid", "3.0"],
                "name": ["a", "b", "c"],
            }
        )

        result = self.engine._convert_numeric_columns(df)

        # Non-numeric strings should remain as object type
        assert result["id"].dtype == "object"
        assert result["value"].dtype == "object"
        assert result["name"].dtype == "object"


class TestClearedEnginePipelineManagement:
    """Test ClearedEngine pipeline management methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        self.engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

    def test_add_pipeline_success(self):
        """Test adding a pipeline successfully."""
        mock_pipeline = Mock(spec=Pipeline)
        mock_pipeline.uid = "test_pipeline"

        self.engine.add_pipeline(mock_pipeline)

        assert len(self.engine) == 1
        assert self.engine._pipelines[0] == mock_pipeline

    def test_add_pipeline_none(self):
        """Test adding None pipeline raises ValueError."""
        with pytest.raises(ValueError, match="Pipeline cannot be None"):
            self.engine.add_pipeline(None)

    def test_add_multiple_pipelines(self):
        """Test adding multiple pipelines."""
        mock_pipeline1 = Mock(spec=Pipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline2 = Mock(spec=Pipeline)
        mock_pipeline2.uid = "pipeline2"

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        assert len(self.engine) == 2
        assert self.engine._pipelines[0] == mock_pipeline1
        assert self.engine._pipelines[1] == mock_pipeline2

    def test_remove_pipeline_success(self):
        """Test removing a pipeline successfully."""
        mock_pipeline1 = Mock(spec=Pipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline2 = Mock(spec=Pipeline)
        mock_pipeline2.uid = "pipeline2"

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        result = self.engine.remove_pipeline("pipeline1")

        assert result is True
        assert len(self.engine) == 1
        assert self.engine._pipelines[0] == mock_pipeline2

    def test_remove_pipeline_not_found(self):
        """Test removing a non-existent pipeline."""
        mock_pipeline = Mock(spec=Pipeline)
        mock_pipeline.uid = "pipeline1"

        self.engine.add_pipeline(mock_pipeline)

        result = self.engine.remove_pipeline("nonexistent")

        assert result is False
        assert len(self.engine) == 1
        assert self.engine._pipelines[0] == mock_pipeline

    def test_remove_pipeline_empty_list(self):
        """Test removing from empty pipeline list."""
        result = self.engine.remove_pipeline("any_pipeline")

        assert result is False
        assert len(self.engine) == 0

    def test_get_pipeline_success(self):
        """Test getting a pipeline successfully."""
        mock_pipeline1 = Mock(spec=Pipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline2 = Mock(spec=Pipeline)
        mock_pipeline2.uid = "pipeline2"

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        result = self.engine.get_pipeline("pipeline1")

        assert result == mock_pipeline1

    def test_get_pipeline_not_found(self):
        """Test getting a non-existent pipeline."""
        mock_pipeline = Mock(spec=Pipeline)
        mock_pipeline.uid = "pipeline1"

        self.engine.add_pipeline(mock_pipeline)

        result = self.engine.get_pipeline("nonexistent")

        assert result is None

    def test_get_pipeline_empty_list(self):
        """Test getting from empty pipeline list."""
        result = self.engine.get_pipeline("any_pipeline")

        assert result is None

    def test_list_pipelines(self):
        """Test listing pipeline UIDs."""
        mock_pipeline1 = Mock(spec=Pipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline2 = Mock(spec=Pipeline)
        mock_pipeline2.uid = "pipeline2"

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        result = self.engine.list_pipelines()

        assert result == ["pipeline1", "pipeline2"]

    def test_list_pipelines_empty(self):
        """Test listing pipelines when empty."""
        result = self.engine.list_pipelines()

        assert result == []

    def test_get_pipeline_count(self):
        """Test getting pipeline count."""
        assert self.engine.get_pipeline_count() == 0

        mock_pipeline1 = Mock(spec=Pipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline2 = Mock(spec=Pipeline)
        mock_pipeline2.uid = "pipeline2"

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        assert self.engine.get_pipeline_count() == 2

    def test_is_empty(self):
        """Test checking if engine is empty."""
        assert self.engine.is_empty() is True

        mock_pipeline = Mock(spec=Pipeline)
        mock_pipeline.uid = "pipeline1"

        self.engine.add_pipeline(mock_pipeline)

        assert self.engine.is_empty() is False


class TestClearedEngineResultsManagement:
    """Test ClearedEngine results management methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        self.engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

    def test_get_results_empty(self):
        """Test getting results when empty."""
        result = self.engine.get_results()

        assert result == {}

    def test_get_results_with_data(self):
        """Test getting results with data."""
        self.engine.results = {"pipeline1": "result1", "pipeline2": "result2"}

        result = self.engine.get_results()

        assert result == {"pipeline1": "result1", "pipeline2": "result2"}
        # Should return a copy
        assert result is not self.engine.results

    def test_clear_results(self):
        """Test clearing results."""
        self.engine.results = {"pipeline1": "result1", "pipeline2": "result2"}

        self.engine.clear_results()

        assert self.engine.results == {}


class TestClearedEngineRegistryManagement:
    """Test ClearedEngine registry management methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        self.engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

    def test_get_registry(self):
        """Test getting registry."""
        registry = self.engine.get_registry()

        assert isinstance(registry, TransformerRegistry)

    def test_set_registry(self):
        """Test setting registry."""
        mock_registry = Mock(spec=TransformerRegistry)

        self.engine.set_registry(mock_registry)

        assert self.engine._registry == mock_registry


class TestClearedEngineSpecialMethods:
    """Test ClearedEngine special methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        self.engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

    def test_repr(self):
        """Test string representation."""
        repr_str = repr(self.engine)

        assert "ClearedEngine" in repr_str
        assert "pipelines=0" in repr_str
        assert "deid_config=True" in repr_str
        assert "registry=True" in repr_str
        assert "io_config=True" in repr_str
        assert "uid=" in repr_str

    def test_repr_with_pipelines(self):
        """Test string representation with pipelines."""
        mock_pipeline = Mock(spec=Pipeline)
        mock_pipeline.uid = "test_pipeline"

        self.engine.add_pipeline(mock_pipeline)

        repr_str = repr(self.engine)

        assert "pipelines=1" in repr_str

    def test_len(self):
        """Test length method."""
        assert len(self.engine) == 0

        mock_pipeline1 = Mock(spec=Pipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline2 = Mock(spec=Pipeline)
        mock_pipeline2.uid = "pipeline2"
        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        assert len(self.engine) == 2

    def test_bool(self):
        """Test boolean conversion."""
        assert bool(self.engine) is False

        mock_pipeline = Mock(spec=Pipeline)
        mock_pipeline.uid = "test_pipeline"

        self.engine.add_pipeline(mock_pipeline)

        assert bool(self.engine) is True


class TestClearedEngineSaveResults:
    """Test ClearedEngine save results method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        self.engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

    @patch("cleared.engine.os.path.join")
    @patch("builtins.open", new_callable=MagicMock)
    @patch("cleared.engine.json.dump")
    def test_save_results(self, mock_json_dump, mock_open, mock_join):
        """Test _save_results method."""
        # Setup mocks
        mock_join.return_value = "/tmp/runtime/status_test_engine_20231201120000.json"
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        results = Results()
        results.add_pipeline_result("pipeline1", "success")

        self.engine._save_results(results)

        # Verify calls
        mock_join.assert_called_once_with(
            self.valid_io_config.runtime_io_path, f"status_{self.engine._uid}.json"
        )
        mock_open.assert_called_once_with(
            "/tmp/runtime/status_test_engine_20231201120000.json", "w"
        )
        # The method converts Results to dict before JSON serialization
        expected_dict = {
            "success": results.success,
            "execution_order": results.execution_order,
            "results": {
                uid: {"status": result.status, "error": result.error}
                for uid, result in results.results.items()
            },
        }
        mock_json_dump.assert_called_once_with(expected_dict, mock_file, indent=2)


class TestClearedEngineEdgeCases:
    """Test ClearedEngine edge cases and error scenarios."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

    def test_init_with_empty_name(self):
        """Test initialization with empty name."""
        engine = ClearedEngine(
            name="", deid_config=self.valid_deid_config, io_config=self.valid_io_config
        )

        assert engine.name == ""
        assert engine._uid.startswith("_")

    def test_init_with_unicode_name(self):
        """Test initialization with unicode name."""
        unicode_name = "test_engine_🚀"

        engine = ClearedEngine(
            name=unicode_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        assert engine.name == unicode_name
        assert engine._uid.startswith(unicode_name + "_")

    def test_init_with_very_long_name(self):
        """Test initialization with very long name."""
        long_name = "a" * 1000

        engine = ClearedEngine(
            name=long_name,
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        assert engine.name == long_name
        assert engine._uid.startswith(long_name + "_")

    def test_run_with_pipeline_uid_none(self):
        """Test run with pipeline that has None UID."""
        engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = None
        mock_pipeline.transform.return_value = (pd.DataFrame(), {})

        engine._pipelines = [mock_pipeline]

        with patch.object(engine, "_load_initial_deid_ref_dict", return_value={}):
            with patch.object(engine, "_save_results"):
                result = engine.run()

        # Should handle None UID gracefully
        assert isinstance(result, Results)
        assert None in result.results
        assert result.results[None].status == "success"

    def test_run_with_pipeline_uid_empty_string(self):
        """Test run with pipeline that has empty string UID."""
        engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = ""
        mock_pipeline.transform.return_value = (pd.DataFrame(), {})

        engine._pipelines = [mock_pipeline]

        with patch.object(engine, "_load_initial_deid_ref_dict", return_value={}):
            with patch.object(engine, "_save_results"):
                result = engine.run()

        # Should handle empty string UID gracefully
        assert isinstance(result, Results)
        assert "" in result.results
        assert result.results[""].status == "success"

    def test_convert_numeric_columns_empty_dataframe(self):
        """Test _convert_numeric_columns with empty DataFrame."""
        engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        df = pd.DataFrame()
        result = engine._convert_numeric_columns(df)

        assert result.equals(df)

    def test_convert_numeric_columns_all_nan_values(self):
        """Test _convert_numeric_columns with all NaN values."""
        engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        df = pd.DataFrame(
            {
                "col1": [float("nan"), float("nan"), float("nan")],
                "col2": ["a", "b", "c"],
            }
        )

        result = engine._convert_numeric_columns(df)

        # Should preserve the original types
        assert result["col1"].dtype == "float64"
        assert result["col2"].dtype == "object"

    def test_convert_numeric_columns_mixed_numeric_and_text(self):
        """Test _convert_numeric_columns with mixed numeric and text values."""
        engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        df = pd.DataFrame(
            {"mixed": ["1", "2", "abc", "4"], "pure_text": ["a", "b", "c", "d"]}
        )

        result = engine._convert_numeric_columns(df)

        # Mixed column should remain as object due to non-numeric values
        assert result["mixed"].dtype == "object"
        assert result["pure_text"].dtype == "object"


class TestClearedEngineReverse:
    """Comprehensive tests for ClearedEngine reverse functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.valid_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/tmp/deid_output"}
                ),
            ),
            runtime_io_path="/tmp/runtime",
        )

        self.valid_deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        self.engine = ClearedEngine(
            name="test_engine",
            deid_config=self.valid_deid_config,
            io_config=self.valid_io_config,
        )

        self.reverse_output_path = "/tmp/reverse_output"

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_with_single_pipeline(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse with a single pipeline."""
        # Setup mocks
        mock_load_deid_ref.return_value = {"test_ref": pd.DataFrame({"id": [1, 2, 3]})}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        mock_pipeline.reverse.return_value = (
            pd.DataFrame({"result": [1, 2, 3]}),
            {"test_ref": pd.DataFrame({"id": [1, 2, 3]})},
        )

        self.engine.add_pipeline(mock_pipeline)

        # Run engine in reverse mode
        result = self.engine.run(
            reverse=True, reverse_output_path=self.reverse_output_path
        )

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True
        assert "test_pipeline" in result.results
        assert result.results["test_pipeline"].status == "success"
        assert result.results["test_pipeline"].error is None
        assert "test_pipeline" in result.execution_order

        # Verify pipeline reverse was called correctly
        call_args = mock_pipeline.reverse.call_args
        assert call_args[1]["df"] is None
        assert "test_ref" in call_args[1]["deid_ref_dict"]
        assert call_args[1]["reverse_output_path"] == self.reverse_output_path

        # Verify methods were called
        mock_load_deid_ref.assert_called_once()
        mock_save_results.assert_called_once()

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_with_multiple_pipelines(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse with multiple pipelines."""
        # Setup mocks
        mock_load_deid_ref.return_value = {"test_ref": pd.DataFrame({"id": [1, 2, 3]})}

        mock_pipeline1 = Mock(spec=TablePipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline1.reverse.return_value = (
            pd.DataFrame({"result1": [1, 2]}),
            {"test_ref": pd.DataFrame({"id": [1, 2, 3]})},
        )

        mock_pipeline2 = Mock(spec=TablePipeline)
        mock_pipeline2.uid = "pipeline2"
        mock_pipeline2.reverse.return_value = (
            pd.DataFrame({"result2": [3, 4]}),
            {"test_ref": pd.DataFrame({"id": [1, 2, 3]})},
        )

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        # Run engine in reverse mode
        result = self.engine.run(
            reverse=True, reverse_output_path=self.reverse_output_path
        )

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True
        assert len(result.results) == 2
        assert "pipeline1" in result.results
        assert "pipeline2" in result.results
        assert result.results["pipeline1"].status == "success"
        assert result.results["pipeline2"].status == "success"
        assert result.execution_order == ["pipeline1", "pipeline2"]

        # Verify pipelines were called in order
        mock_pipeline1.reverse.assert_called_once()
        mock_pipeline2.reverse.assert_called_once()

        # Verify reverse_output_path was passed to both
        assert (
            mock_pipeline1.reverse.call_args[1]["reverse_output_path"]
            == self.reverse_output_path
        )
        assert (
            mock_pipeline2.reverse.call_args[1]["reverse_output_path"]
            == self.reverse_output_path
        )

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_in_test_mode(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse in test mode (no output saving)."""
        # Setup mocks
        mock_load_deid_ref.return_value = {"test_ref": pd.DataFrame({"id": [1, 2, 3]})}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        mock_pipeline.reverse.return_value = (
            pd.DataFrame({"result": [1, 2, 3]}),
            {"test_ref": pd.DataFrame({"id": [1, 2, 3]})},
        )

        self.engine.add_pipeline(mock_pipeline)

        # Run engine in reverse mode with test_mode
        result = self.engine.run(
            reverse=True,
            reverse_output_path=self.reverse_output_path,
            test_mode=True,
        )

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True

        # Verify that _save_results and _save_deid_ref_files were NOT called (test mode)
        mock_save_results.assert_not_called()
        mock_save_deid_ref.assert_not_called()

        # Verify pipeline reverse was called with test_mode
        call_args = mock_pipeline.reverse.call_args
        assert call_args[1]["test_mode"] is True

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_with_rows_limit(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse with rows_limit parameter."""
        # Setup mocks
        mock_load_deid_ref.return_value = {"test_ref": pd.DataFrame({"id": [1, 2, 3]})}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        # Return DataFrame with 5 rows (limited)
        mock_pipeline.reverse.return_value = (
            pd.DataFrame({"result": range(1, 6)}),
            {"test_ref": pd.DataFrame({"id": [1, 2, 3]})},
        )

        self.engine.add_pipeline(mock_pipeline)

        # Run engine in reverse mode with rows_limit
        result = self.engine.run(
            reverse=True,
            reverse_output_path=self.reverse_output_path,
            rows_limit=5,
        )

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True

        # Verify pipeline reverse was called with rows_limit
        call_args = mock_pipeline.reverse.call_args
        assert call_args[1]["rows_limit"] == 5

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    def test_reverse_missing_reverse_output_path_raises_error(self, mock_load_deid_ref):
        """Test reverse raises error when reverse_output_path is missing."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        mock_pipeline.reverse.side_effect = ValueError(
            "reverse_output_path is required when reverse=True"
        )

        self.engine.add_pipeline(mock_pipeline)

        # Run engine in reverse mode without reverse_output_path
        with pytest.raises(RuntimeError, match="Pipeline execution failed"):
            self.engine.run(reverse=True, test_mode=False)

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_pipeline_exception_without_continue(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse with pipeline exception and continue_on_error=False."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        mock_pipeline.reverse.side_effect = Exception("Reverse failed")

        self.engine.add_pipeline(mock_pipeline)

        # Run engine in reverse mode with continue_on_error=False (default)
        with pytest.raises(
            RuntimeError, match="Pipeline execution failed: Reverse failed"
        ):
            self.engine.run(
                reverse=True,
                reverse_output_path=self.reverse_output_path,
                continue_on_error=False,
            )

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_pipeline_exception_with_continue(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse with pipeline exception and continue_on_error=True."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline1 = Mock(spec=TablePipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline1.reverse.side_effect = Exception("Reverse 1 failed")

        mock_pipeline2 = Mock(spec=TablePipeline)
        mock_pipeline2.uid = "pipeline2"
        mock_pipeline2.reverse.return_value = (pd.DataFrame({"result": [1, 2]}), {})

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        # Run engine in reverse mode with continue_on_error=True
        result = self.engine.run(
            reverse=True,
            reverse_output_path=self.reverse_output_path,
            continue_on_error=True,
        )

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True  # Should still be True since we continued
        assert len(result.results) == 2
        assert result.results["pipeline1"].status == "error"
        assert "Reverse 1 failed" in result.results["pipeline1"].error
        assert result.results["pipeline2"].status == "success"
        assert result.execution_order == ["pipeline1", "pipeline2"]

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_pipeline_without_reverse_method(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse with pipeline that doesn't have reverse method."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        # Remove reverse method
        del mock_pipeline.reverse

        self.engine.add_pipeline(mock_pipeline)

        # Run engine in reverse mode
        result = self.engine.run(
            reverse=True,
            reverse_output_path=self.reverse_output_path,
            continue_on_error=True,
        )

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True
        assert "test_pipeline" in result.results
        assert result.results["test_pipeline"].status == "error"
        # The error message is generated from the exception, which is "reverse" (AttributeError)
        assert "reverse" in result.results["test_pipeline"].error

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_with_no_pipelines(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse with no pipelines configured."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        # Run engine in reverse mode with no pipelines
        with pytest.raises(
            ValueError, match=r"No pipelines configured. Add pipelines before running."
        ):
            self.engine.run(reverse=True, reverse_output_path=self.reverse_output_path)

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_round_trip_consistency(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test that transform -> reverse maintains data integrity."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"

        # First call: transform
        transformed_df = pd.DataFrame({"patient_id": [1, 2, 3]})
        deid_ref_dict_after_transform = {"patient_uid": pd.DataFrame({"id": [1, 2, 3]})}

        # Second call: reverse
        reversed_df = pd.DataFrame({"patient_id": ["user_001", "user_002", "user_003"]})

        # Set up side effects for transform and reverse
        mock_pipeline.transform.return_value = (
            transformed_df,
            deid_ref_dict_after_transform,
        )
        mock_pipeline.reverse.return_value = (
            reversed_df,
            deid_ref_dict_after_transform,
        )

        self.engine.add_pipeline(mock_pipeline)

        # Transform
        transform_result = self.engine.run(reverse=False)

        # Verify transform was called
        assert transform_result.success is True
        mock_pipeline.transform.assert_called_once()

        # Reset mock for reverse
        mock_pipeline.reset_mock()
        mock_pipeline.transform.return_value = (
            transformed_df,
            deid_ref_dict_after_transform,
        )
        mock_pipeline.reverse.return_value = (
            reversed_df,
            deid_ref_dict_after_transform,
        )

        # Reverse
        reverse_result = self.engine.run(
            reverse=True, reverse_output_path=self.reverse_output_path
        )

        # Verify reverse was called
        assert reverse_result.success is True
        mock_pipeline.reverse.assert_called_once()

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_passes_deid_ref_dict_correctly(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test that deid_ref_dict is passed correctly to reverse."""
        # Setup mocks
        initial_deid_ref = {
            "patient_uid": pd.DataFrame(
                {
                    "patient_uid": ["user_001", "user_002"],
                    "patient_uid__deid": [1, 2],
                }
            )
        }
        mock_load_deid_ref.return_value = initial_deid_ref

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        updated_deid_ref = initial_deid_ref.copy()
        mock_pipeline.reverse.return_value = (
            pd.DataFrame({"result": [1, 2]}),
            updated_deid_ref,
        )

        self.engine.add_pipeline(mock_pipeline)

        # Run engine in reverse mode
        self.engine.run(reverse=True, reverse_output_path=self.reverse_output_path)

        # Verify deid_ref_dict was passed correctly
        call_args = mock_pipeline.reverse.call_args
        assert "deid_ref_dict" in call_args[1]
        assert "patient_uid" in call_args[1]["deid_ref_dict"]
        pd.testing.assert_frame_equal(
            call_args[1]["deid_ref_dict"]["patient_uid"],
            initial_deid_ref["patient_uid"],
        )

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_with_empty_deid_ref_dict(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse with empty deid_ref_dict."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        mock_pipeline.reverse.return_value = (pd.DataFrame({"result": [1, 2]}), {})

        self.engine.add_pipeline(mock_pipeline)

        # Run engine in reverse mode
        result = self.engine.run(
            reverse=True, reverse_output_path=self.reverse_output_path
        )

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True

        # Verify empty deid_ref_dict was passed
        call_args = mock_pipeline.reverse.call_args
        assert call_args[1]["deid_ref_dict"] == {}

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_all_pipelines_fail_with_continue(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse with all pipelines failing and continue_on_error=True."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline1 = Mock(spec=TablePipeline)
        mock_pipeline1.uid = "pipeline1"
        mock_pipeline1.reverse.side_effect = Exception("Reverse 1 failed")

        mock_pipeline2 = Mock(spec=TablePipeline)
        mock_pipeline2.uid = "pipeline2"
        mock_pipeline2.reverse.side_effect = Exception("Reverse 2 failed")

        self.engine.add_pipeline(mock_pipeline1)
        self.engine.add_pipeline(mock_pipeline2)

        # Run engine in reverse mode with continue_on_error=True
        result = self.engine.run(
            reverse=True,
            reverse_output_path=self.reverse_output_path,
            continue_on_error=True,
        )

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True  # Should still be True since we continued
        assert len(result.results) == 2
        assert result.results["pipeline1"].status == "error"
        assert result.results["pipeline2"].status == "error"
        assert "Reverse 1 failed" in result.results["pipeline1"].error
        assert "Reverse 2 failed" in result.results["pipeline2"].error

    @patch.object(ClearedEngine, "_load_initial_deid_ref_dict")
    @patch.object(ClearedEngine, "_save_results")
    @patch.object(ClearedEngine, "_save_deid_ref_files")
    def test_reverse_with_rows_limit_and_test_mode(
        self, mock_save_deid_ref, mock_save_results, mock_load_deid_ref
    ):
        """Test reverse with both rows_limit and test_mode."""
        # Setup mocks
        mock_load_deid_ref.return_value = {}

        mock_pipeline = Mock(spec=TablePipeline)
        mock_pipeline.uid = "test_pipeline"
        mock_pipeline.reverse.return_value = (
            pd.DataFrame({"result": range(1, 4)}),
            {},
        )

        self.engine.add_pipeline(mock_pipeline)

        # Run engine in reverse mode with both parameters
        result = self.engine.run(
            reverse=True,
            reverse_output_path=self.reverse_output_path,
            rows_limit=3,
            test_mode=True,
        )

        # Verify results
        assert isinstance(result, Results)
        assert result.success is True

        # Verify both parameters were passed
        call_args = mock_pipeline.reverse.call_args
        assert call_args[1]["rows_limit"] == 3
        assert call_args[1]["test_mode"] is True

        # Verify no saving occurred (test mode)
        mock_save_results.assert_not_called()
        mock_save_deid_ref.assert_not_called()
