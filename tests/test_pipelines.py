"""Comprehensive tests for Pipeline and TablePipeline classes."""

import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import Mock, patch
from omegaconf import DictConfig

from cleared.transformers.base import Pipeline, BaseTransformer
from cleared.transformers.pipelines import TablePipeline
from cleared.transformers.id import IDDeidentifier
from cleared.config.structure import (
    IOConfig,
    DeIDConfig,
    IdentifierConfig,
    PairedIOConfig,
)
from cleared.io.base import BaseDataLoader


class MockTransformer(BaseTransformer):
    """Mock transformer for testing."""

    def __init__(
        self, uid: str = "mock_transformer", dependencies: list[str] | None = None
    ):
        """Initialize the mock transformer."""
        super().__init__(uid, dependencies)
        self.transform_called = False
        self.last_df = None
        self.last_deid_ref_dict = None

    def transform(
        self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """Mock transform method."""
        self.transform_called = True
        self.last_df = df.copy()
        self.last_deid_ref_dict = (
            deid_ref_dict.copy() if deid_ref_dict is not None else None
        )

        # Simple transformation: add a column
        result_df = df.copy()
        result_df["transformed"] = True
        return result_df, deid_ref_dict.copy() if deid_ref_dict is not None else {}


class MockDataLoader(BaseDataLoader):
    """Mock data loader for testing."""

    def __init__(self, config):
        """
        Initialize the mock data loader.

        Args:
            config: Configuration for the data loader.

        Returns:
            None

        """
        # Ensure config has required fields
        if not isinstance(config, dict):
            config = {}
        config.setdefault("data_source_type", "mock")
        config.setdefault("connection_params", {})
        config.setdefault("table_mappings", {})
        config.setdefault("suffix", "_deid")
        config.setdefault("validation_rules", {})

        # Convert to DictConfig

        config = DictConfig(config)

        super().__init__(config)
        self.data = {}
        self.read_called = False
        self.write_called = False
        self.last_table_name = None
        self.last_df = None

    def _initialize_connection(self):
        """Mock connection initialization."""
        pass

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Mock read table."""
        self.read_called = True
        self.last_table_name = table_name
        if table_name in self.data:
            return self.data[table_name].copy()
        else:
            from cleared.io.base import TableNotFoundError

            raise TableNotFoundError(f"Table {table_name} not found")

    def write_deid_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False,
    ):
        """Mock write table."""
        self.write_called = True
        self.last_table_name = table_name
        self.last_df = df.copy()
        self.data[table_name] = df.copy()

    def list_tables(self):
        """Mock list tables."""
        return list(self.data.keys())

    def close_connection(self):
        """Mock close connection."""
        pass


class TestPipeline:
    """Test the base Pipeline class."""

    def test_initialization_with_defaults(self):
        """Test pipeline initialization with default parameters."""
        pipeline = Pipeline()

        assert pipeline.uid is not None
        assert isinstance(pipeline.uid, str)
        assert pipeline.transformers == ()
        assert pipeline.dependencies == []

    def test_initialization_with_parameters(self):
        """Test pipeline initialization with custom parameters."""
        transformer1 = MockTransformer("transformer1")
        transformer2 = MockTransformer("transformer2")
        dependencies = ["dep1", "dep2"]

        pipeline = Pipeline(
            uid="test_pipeline",
            transformers=[transformer1, transformer2],
            dependencies=dependencies,
        )

        assert pipeline.uid == "test_pipeline"
        assert len(pipeline.transformers) == 2
        assert pipeline.transformers[0].uid == "transformer1"
        assert pipeline.transformers[1].uid == "transformer2"
        assert pipeline.dependencies == dependencies

    def test_add_transformer(self):
        """Test adding transformers to pipeline."""
        pipeline = Pipeline()
        transformer = MockTransformer("test_transformer")

        pipeline.add_transformer(transformer)

        assert len(pipeline.transformers) == 1
        assert pipeline.transformers[0].uid == "test_transformer"

    def test_add_transformer_none(self):
        """Test adding None transformer raises error."""
        pipeline = Pipeline()

        with pytest.raises(
            ValueError, match="Transformer must be specified and must not be None"
        ):
            pipeline.add_transformer(None)

    def test_transform_no_transformers(self):
        """Test transform with no transformers."""
        pipeline = Pipeline()
        df = pd.DataFrame({"col1": [1, 2, 3]})
        deid_ref_dict = {"test": pd.DataFrame({"ref_col": ["a", "b", "c"]})}

        result_df, result_deid_ref_dict = pipeline.transform(df, deid_ref_dict)

        pd.testing.assert_frame_equal(result_df, df)
        assert result_deid_ref_dict == deid_ref_dict

    def test_transform_with_transformers(self):
        """Test transform with transformers."""
        pipeline = Pipeline()
        transformer1 = MockTransformer("transformer1")
        transformer2 = MockTransformer("transformer2")

        pipeline.add_transformer(transformer1)
        pipeline.add_transformer(transformer2)

        df = pd.DataFrame({"col1": [1, 2, 3]})
        deid_ref_dict = {"test": pd.DataFrame({"ref_col": ["a", "b", "c"]})}

        result_df, _ = pipeline.transform(df, deid_ref_dict)

        # Check that transformers were called
        assert transformer1.transform_called
        assert transformer2.transform_called

        # Check that the result has the transformed column
        assert "transformed" in result_df.columns
        assert result_df["transformed"].all()

    def test_transform_with_dependencies(self):
        """Test transform with transformer dependencies."""
        # Create transformers with dependencies
        transformer1 = MockTransformer("transformer1")
        transformer2 = MockTransformer("transformer2", dependencies=["transformer1"])
        transformer3 = MockTransformer("transformer3", dependencies=["transformer2"])

        pipeline = Pipeline(transformers=[transformer1, transformer2, transformer3])

        df = pd.DataFrame({"col1": [1, 2, 3]})
        deid_ref_dict = {"test": pd.DataFrame({"ref_col": ["a", "b", "c"]})}

        result_df, _ = pipeline.transform(df, deid_ref_dict)

        # All transformers should be called
        assert transformer1.transform_called
        assert transformer2.transform_called
        assert transformer3.transform_called

        # Result should have transformed column
        assert "transformed" in result_df.columns

    def test_transform_none_dataframe(self):
        """Test transform with None DataFrame raises error."""
        pipeline = Pipeline()
        deid_ref_dict = {"test": pd.DataFrame({"ref_col": ["a", "b", "c"]})}

        with pytest.raises(ValueError, match="DataFrame is required"):
            pipeline.transform(None, deid_ref_dict)

    def test_transform_none_deid_ref_dict(self):
        """Test transform with None deid_ref_dict raises error."""
        pipeline = Pipeline()
        df = pd.DataFrame({"col1": [1, 2, 3]})

        with pytest.raises(
            ValueError, match="De-identification reference dictionary is required"
        ):
            pipeline.transform(df, None)

    def test_transformers_property(self):
        """Test transformers property returns tuple."""
        pipeline = Pipeline()
        transformer = MockTransformer("test_transformer")
        pipeline.add_transformer(transformer)

        transformers = pipeline.transformers
        assert isinstance(transformers, tuple)
        assert len(transformers) == 1
        assert transformers[0].uid == "test_transformer"

    def test_circular_dependency_handling(self):
        """Test that circular dependencies are detected and raise an error."""
        transformer1 = MockTransformer("transformer1", dependencies=["transformer2"])
        transformer2 = MockTransformer("transformer2", dependencies=["transformer1"])

        pipeline = Pipeline(transformers=[transformer1, transformer2])

        df = pd.DataFrame({"col1": [1, 2, 3]})
        deid_ref_dict = {"test": pd.DataFrame({"ref_col": ["a", "b", "c"]})}

        # Should raise NetworkXUnfeasible due to circular dependency
        import networkx

        with pytest.raises(
            networkx.exception.NetworkXUnfeasible, match="Graph contains a cycle"
        ):
            pipeline.transform(df, deid_ref_dict)


class TestTablePipeline:
    """Test the TablePipeline class."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.table_name = "test_table"

        # Create test data
        self.test_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "admission_date": ["2023-01-15", "2023-02-20", "2023-03-10"],
            }
        )
        self.test_df.to_csv(
            os.path.join(self.temp_dir, f"{self.table_name}.csv"), index=False
        )

        # Create configs
        input_io_config = IOConfig(
            io_type="filesystem",
            configs={"base_path": self.temp_dir, "file_format": "csv"},
        )
        output_io_config = IOConfig(
            io_type="filesystem",
            configs={"base_path": self.temp_dir, "file_format": "csv"},
        )
        self.io_config = PairedIOConfig(
            input_config=input_io_config, output_config=output_io_config
        )

        self.deid_config = DeIDConfig(time_shift=None)

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("cleared.io.create_data_loader")
    def test_initialization(self, mock_create_loader):
        """Test TablePipeline initialization."""
        mock_loader = MockDataLoader({})
        mock_create_loader.return_value = mock_loader

        pipeline = TablePipeline(
            table_name=self.table_name,
            io_config=self.io_config,
            deid_config=self.deid_config,
        )

        assert pipeline.table_name == self.table_name
        assert pipeline.io_config == self.io_config
        assert pipeline.deid_config == self.deid_config

    @patch("cleared.io.create_data_loader")
    def test_initialization_with_custom_parameters(self, mock_create_loader):
        """Test TablePipeline initialization with custom parameters."""
        mock_loader = MockDataLoader({})
        mock_create_loader.return_value = mock_loader

        transformer = MockTransformer("test_transformer")

        pipeline = TablePipeline(
            table_name=self.table_name,
            io_config=self.io_config,
            deid_config=self.deid_config,
            uid="custom_pipeline",
            dependencies=["dep1", "dep2"],
            transformers=[transformer],
        )

        assert pipeline.uid == "custom_pipeline"
        assert pipeline.dependencies == ["dep1", "dep2"]
        assert len(pipeline.transformers) == 1
        assert pipeline.transformers[0].uid == "test_transformer"

    @patch("cleared.io.create_data_loader")
    def test_transform_with_data_loading(self, mock_create_loader):
        """Test transform with automatic data loading."""
        mock_loader = MockDataLoader({})
        mock_loader.data[self.table_name] = self.test_df.copy()
        mock_create_loader.return_value = mock_loader

        pipeline = TablePipeline(
            table_name=self.table_name,
            io_config=self.io_config,
            deid_config=self.deid_config,
        )

        # Add a transformer
        transformer = MockTransformer("test_transformer")
        pipeline.add_transformer(transformer)

        # Call transform to trigger data loading and transformation
        _, _ = pipeline.transform()

        # Check that data was read
        assert mock_loader.read_called
        assert mock_loader.last_table_name == self.table_name

        # Check that transformer was called
        assert transformer.transform_called

        # Check that data was written
        assert mock_loader.write_called
        assert mock_loader.last_table_name == self.table_name

    @patch("cleared.io.create_data_loader")
    def test_transform_with_provided_data(self, mock_create_loader):
        """Test transform with provided DataFrame."""
        mock_loader = MockDataLoader({})
        mock_create_loader.return_value = mock_loader

        pipeline = TablePipeline(
            table_name=self.table_name,
            io_config=self.io_config,
            deid_config=self.deid_config,
        )

        # Add a transformer
        transformer = MockTransformer("test_transformer")
        pipeline.add_transformer(transformer)

        # Call transform with provided data
        _, _ = pipeline.transform(self.test_df)

        # Check that transformer was called with provided data
        assert transformer.transform_called
        pd.testing.assert_frame_equal(transformer.last_df, self.test_df)

        # Check that data was written
        assert mock_loader.write_called

    @patch("cleared.io.create_data_loader")
    def test_transform_read_error(self, mock_create_loader):
        """Test transform when data loading fails."""
        mock_loader = Mock()
        mock_loader.read_table.side_effect = Exception("Read error")
        mock_create_loader.return_value = mock_loader

        pipeline = TablePipeline(
            table_name=self.table_name,
            io_config=self.io_config,
            deid_config=self.deid_config,
        )

        deid_ref_dict = {"test": pd.DataFrame({"ref_col": ["a", "b", "c"]})}

        with pytest.raises(ValueError, match="Failed to read table"):
            pipeline.transform(deid_ref_dict=deid_ref_dict)

    def test_integration_with_real_transformers(self):
        """Test integration with real transformer classes."""
        # This test uses real transformers but mocks the data loader
        with patch("cleared.io.create_data_loader") as mock_create_loader:
            mock_loader = MockDataLoader({})
            mock_loader.data[self.table_name] = self.test_df.copy()
            mock_create_loader.return_value = mock_loader

            pipeline = TablePipeline(
                table_name=self.table_name,
                io_config=self.io_config,
                deid_config=self.deid_config,
            )

            # Add real transformers
            id_config = IdentifierConfig(
                name="patient_id", uid="patient_id", description="Patient identifier"
            )
            id_transformer = IDDeidentifier(id_config, uid="id_deidentifier")
            pipeline.add_transformer(id_transformer)

            # Create a simple deid_ref_dict
            deid_ref_dict = {
                "patient_id": pd.DataFrame(
                    {
                        "patient_id": [1, 2, 3],
                        "patient_id__deid": ["deid_1", "deid_2", "deid_3"],
                    }
                )
            }

            # Transform
            result_df, _ = pipeline.transform(deid_ref_dict=deid_ref_dict)
            # Check that data was processed
            assert len(result_df) == 3
            assert result_df.patient_id.equals(
                pd.Series(["deid_1", "deid_2", "deid_3"])
            )
            assert "patient_id" in result_df.columns
            assert mock_loader.write_called


class TestTablePipelineEdgeCases:
    """Test edge cases for TablePipeline."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.table_name = "test_table"

        input_io_config = IOConfig(
            io_type="filesystem",
            configs={"base_path": self.temp_dir, "file_format": "csv"},
        )
        output_io_config = IOConfig(
            io_type="filesystem",
            configs={"base_path": self.temp_dir, "file_format": "csv"},
        )
        self.io_config = PairedIOConfig(
            input_config=input_io_config, output_config=output_io_config
        )

        self.deid_config = DeIDConfig(time_shift=None)

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("cleared.io.create_data_loader")
    def test_empty_dataframe(self, mock_create_loader):
        """Test with empty DataFrame."""
        mock_loader = MockDataLoader({})
        mock_loader.data[self.table_name] = pd.DataFrame()
        mock_create_loader.return_value = mock_loader

        pipeline = TablePipeline(
            table_name=self.table_name,
            io_config=self.io_config,
            deid_config=self.deid_config,
        )

        deid_ref_dict = pd.DataFrame()
        result_df, _ = pipeline.transform(deid_ref_dict=deid_ref_dict)

        assert len(result_df) == 0
        assert mock_loader.write_called

    @patch("cleared.io.create_data_loader")
    def test_large_dataframe(self, mock_create_loader):
        """Test with large DataFrame."""
        mock_loader = MockDataLoader({})

        # Create large DataFrame
        large_df = pd.DataFrame(
            {"id": range(10000), "value": [f"value_{i}" for i in range(10000)]}
        )
        mock_loader.data[self.table_name] = large_df
        mock_create_loader.return_value = mock_loader

        pipeline = TablePipeline(
            table_name=self.table_name,
            io_config=self.io_config,
            deid_config=self.deid_config,
        )

        deid_ref_dict = pd.DataFrame()
        result_df, _ = pipeline.transform(deid_ref_dict=deid_ref_dict)

        assert len(result_df) == 10000
        assert mock_loader.write_called

    @patch("cleared.io.create_data_loader")
    def test_multiple_transformers_chain(self, mock_create_loader):
        """Test with multiple transformers in chain."""
        mock_loader = MockDataLoader({})
        test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_loader.data[self.table_name] = test_df
        mock_create_loader.return_value = mock_loader

        pipeline = TablePipeline(
            table_name=self.table_name,
            io_config=self.io_config,
            deid_config=self.deid_config,
        )

        # Add multiple transformers
        transformer1 = MockTransformer("transformer1")
        transformer2 = MockTransformer("transformer2")
        transformer3 = MockTransformer("transformer3")

        pipeline.add_transformer(transformer1)
        pipeline.add_transformer(transformer2)
        pipeline.add_transformer(transformer3)

        deid_ref_dict = pd.DataFrame()
        result_df, _ = pipeline.transform(deid_ref_dict=deid_ref_dict)

        # All transformers should be called
        assert transformer1.transform_called
        assert transformer2.transform_called
        assert transformer3.transform_called

        # Result should have multiple transformed columns
        assert "transformed" in result_df.columns
        assert result_df["transformed"].all()


class TestPipelineIntegration:
    """Integration tests for Pipeline classes."""

    def test_pipeline_inheritance(self):
        """Test that TablePipeline properly inherits from Pipeline."""
        assert issubclass(TablePipeline, Pipeline)
        assert issubclass(TablePipeline, BaseTransformer)

    def test_pipeline_polymorphism(self):
        """Test that TablePipeline can be used as Pipeline."""
        with patch("cleared.io.create_data_loader") as mock_create_loader:
            mock_loader = MockDataLoader({})
            mock_create_loader.return_value = mock_loader

            input_io_config = IOConfig(io_type="filesystem", configs={})
            output_io_config = IOConfig(io_type="filesystem", configs={})
            io_config = PairedIOConfig(
                input_config=input_io_config, output_config=output_io_config
            )
            deid_config = DeIDConfig(time_shift=None)

            table_pipeline = TablePipeline(
                table_name="test", io_config=io_config, deid_config=deid_config
            )

            # Should be usable as Pipeline
            pipeline: Pipeline = table_pipeline
            assert isinstance(pipeline, Pipeline)
            assert isinstance(pipeline, BaseTransformer)

    def test_pipeline_uid_uniqueness(self):
        """Test that pipeline UIDs are unique."""
        pipeline1 = Pipeline()
        pipeline2 = Pipeline()

        assert pipeline1.uid != pipeline2.uid

    def test_pipeline_uid_persistence(self):
        """Test that pipeline UID persists across operations."""
        pipeline = Pipeline(uid="test_pipeline")

        assert pipeline.uid == "test_pipeline"

        # Add transformers and transform
        transformer = MockTransformer()
        pipeline.add_transformer(transformer)

        df = pd.DataFrame({"col1": [1, 2, 3]})
        deid_ref_dict = {"test": pd.DataFrame({"ref_col": ["a", "b", "c"]})}
        pipeline.transform(df, deid_ref_dict)

        # UID should still be the same
        assert pipeline.uid == "test_pipeline"
