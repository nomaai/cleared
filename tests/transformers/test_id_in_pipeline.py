"""Unit tests for IDDeidentifier in pipeline scenarios."""

import pandas as pd
import numpy as np
import tempfile
from typing import Union
from unittest.mock import patch
from omegaconf import DictConfig

from cleared.transformers.id import IDDeidentifier
from cleared.transformers.pipelines import TablePipeline
from cleared.config.structure import (
    IdentifierConfig,
    IOConfig,
    DeIDConfig,
    PairedIOConfig,
)
from cleared.io.base import BaseDataLoader


class MockDataLoader(BaseDataLoader):
    """Mock data loader for testing."""

    def __init__(self, config: Union[dict, DictConfig]):  # noqa: UP007
        """
        Initialize the mock data loader.

        Args:
            config: Configuration for the data loader.

        Returns:
            None

        Raises:
            ValueError: If the configuration is not a dictionary or DictConfig.

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


class TestIDDeidentifierInPipeline:
    """Test IDDeidentifier in various pipeline scenarios."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()

        # Create test data
        self.patients_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "age": [25, 30, 35, 28, 32],
            }
        )

        self.encounters_df = pd.DataFrame(
            {
                "encounter_id": [101, 102, 103, 104, 105],
                "patient_id": [1, 2, 1, 3, 4],  # Some overlap with patients
                "diagnosis": ["Cold", "Fever", "Headache", "Cough", "Flu"],
            }
        )

        self.visits_df = pd.DataFrame(
            {
                "visit_id": [201, 202, 203, 204, 205],
                "patient_id": [1, 2, 6, 7, 8],  # Different patient IDs
                "visit_date": [
                    "2023-01-01",
                    "2023-01-02",
                    "2023-01-03",
                    "2023-01-04",
                    "2023-01-05",
                ],
            }
        )

        self.mixed_ids_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "encounter_id": [101, 102, 103, 104, 105],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "diagnosis": ["Cold", "Fever", "Headache", "Cough", "Flu"],
            }
        )

        # Create base configs
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
    def test_single_table_single_id_deidentification(self, mock_create_loader):
        """Test IDDeidentifier with one table and one ID column."""
        # Setup mock data loader
        mock_loader = MockDataLoader({})
        mock_loader.data["patients"] = self.patients_df.copy()
        mock_create_loader.return_value = mock_loader

        # Create IDDeidentifier transformer
        id_config = IdentifierConfig(
            name="patient_id", uid="patient_id", description="Patient identifier"
        )
        id_transformer = IDDeidentifier(id_config, uid="patient_id_transformer")

        # Create table pipeline
        pipeline = TablePipeline(
            table_name="patients",
            io_config=self.io_config,
            deid_config=self.deid_config,
        )
        pipeline.add_transformer(id_transformer)

        # Start with empty deid_ref_dict
        deid_ref_dict = {}

        # Transform the data
        result_df, result_deid_ref_dict = pipeline.transform(
            deid_ref_dict=deid_ref_dict
        )

        # Verify results
        assert len(result_df) == len(self.patients_df)
        assert "patient_id" in result_df.columns
        assert "name" in result_df.columns
        assert "age" in result_df.columns

        # Check that patient_id values are de-identified (sequential integers)
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in result_df["patient_id"]
        )
        # Check that values are sequential starting from 1
        deid_values = sorted(result_df["patient_id"].tolist())
        assert deid_values == [1, 2, 3, 4, 5]

        # Check that deid_ref_dict was updated
        assert "patient_id" in result_deid_ref_dict
        deid_ref_df = result_deid_ref_dict["patient_id"]
        assert len(deid_ref_df) == 5  # 5 unique patient IDs
        assert "patient_id" in deid_ref_df.columns
        assert "patient_id__deid" in deid_ref_df.columns

        # Check that original values are preserved in deid_ref_df
        assert set(deid_ref_df["patient_id"]) == {1, 2, 3, 4, 5}

        # Verify data loader was called
        assert mock_loader.read_called
        assert mock_loader.write_called

    @patch("cleared.io.create_data_loader")
    def test_two_tables_different_id_columns(self, mock_create_loader):
        """Test IDDeidentifier with two tables having different ID columns."""
        # Setup mock data loader
        mock_loader = MockDataLoader({})
        mock_loader.data["patients"] = self.patients_df.copy()
        mock_loader.data["encounters"] = self.encounters_df.copy()
        mock_create_loader.return_value = mock_loader

        # Create IDDeidentifier transformers for different ID columns
        patient_id_config = IdentifierConfig(
            name="patient_id", uid="patient_id", description="Patient identifier"
        )
        patient_id_transformer = IDDeidentifier(
            patient_id_config, uid="patient_id_transformer"
        )

        encounter_id_config = IdentifierConfig(
            name="encounter_id", uid="encounter_id", description="Encounter identifier"
        )
        encounter_id_transformer = IDDeidentifier(
            encounter_id_config, uid="encounter_id_transformer"
        )

        # Create table pipelines
        patients_pipeline = TablePipeline(
            table_name="patients",
            io_config=self.io_config,
            deid_config=self.deid_config,
        )
        patients_pipeline.add_transformer(patient_id_transformer)

        encounters_pipeline = TablePipeline(
            table_name="encounters",
            io_config=self.io_config,
            deid_config=self.deid_config,
        )
        encounters_pipeline.add_transformer(encounter_id_transformer)

        # Start with empty deid_ref_dict
        deid_ref_dict = {}

        # Transform both tables
        patients_result, deid_ref_dict = patients_pipeline.transform(
            deid_ref_dict=deid_ref_dict
        )
        encounters_result, deid_ref_dict = encounters_pipeline.transform(
            deid_ref_dict=deid_ref_dict
        )

        # Verify patients table results
        assert len(patients_result) == len(self.patients_df)
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in patients_result["patient_id"]
        )
        # Check that values are sequential
        patient_deid_values = sorted(patients_result["patient_id"].tolist())
        assert patient_deid_values == [1, 2, 3, 4, 5]

        # Verify encounters table results
        assert len(encounters_result) == len(self.encounters_df)
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in encounters_result["encounter_id"]
        )
        # Check that values are sequential (each transformer starts from 1)
        encounter_deid_values = sorted(encounters_result["encounter_id"].tolist())
        assert encounter_deid_values == [
            1,
            2,
            3,
            4,
            5,
        ]  # Each transformer starts sequential numbering from 1

        # Check that deid_ref_dict has both transformers' data
        assert "patient_id" in deid_ref_dict
        assert "encounter_id" in deid_ref_dict

        # Verify patient_id deid_ref_df
        patient_deid_df = deid_ref_dict["patient_id"]
        assert len(patient_deid_df) == 5
        assert set(patient_deid_df["patient_id"]) == {1, 2, 3, 4, 5}

        # Verify encounter_id deid_ref_df
        encounter_deid_df = deid_ref_dict["encounter_id"]
        assert len(encounter_deid_df) == 5
        assert set(encounter_deid_df["encounter_id"]) == {101, 102, 103, 104, 105}

    @patch("cleared.io.create_data_loader")
    def test_two_tables_same_id_column_partial_overlap(self, mock_create_loader):
        """Test IDDeidentifier with two tables having same ID column but partial overlap."""
        # Create overlapping data
        patients_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            }
        )

        visits_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 6, 7, 8],  # Overlap: 1, 2; New: 6, 7, 8
                "visit_date": [
                    "2023-01-01",
                    "2023-01-02",
                    "2023-01-03",
                    "2023-01-04",
                    "2023-01-05",
                ],
            }
        )

        # Setup mock data loader
        mock_loader = MockDataLoader({})
        mock_loader.data["patients"] = patients_df.copy()
        mock_loader.data["visits"] = visits_df.copy()
        mock_create_loader.return_value = mock_loader

        # Create IDDeidentifier transformer for patient_id
        id_config = IdentifierConfig(
            name="patient_id", uid="patient_id", description="Patient identifier"
        )
        id_transformer = IDDeidentifier(id_config, uid="patient_id_transformer")

        # Create table pipelines
        patients_pipeline = TablePipeline(
            table_name="patients",
            io_config=self.io_config,
            deid_config=self.deid_config,
        )
        patients_pipeline.add_transformer(id_transformer)

        visits_pipeline = TablePipeline(
            table_name="visits", io_config=self.io_config, deid_config=self.deid_config
        )
        visits_pipeline.add_transformer(id_transformer)

        # Start with empty deid_ref_dict
        deid_ref_dict = {}

        # Transform both tables
        patients_result, deid_ref_dict = patients_pipeline.transform(
            deid_ref_dict=deid_ref_dict
        )
        visits_result, deid_ref_dict = visits_pipeline.transform(
            deid_ref_dict=deid_ref_dict
        )

        # Verify patients table results
        assert len(patients_result) == len(patients_df)
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in patients_result["patient_id"]
        )
        # Check that values are sequential
        patient_deid_values = sorted(patients_result["patient_id"].tolist())
        assert patient_deid_values == [1, 2, 3, 4, 5]

        # Verify visits table results
        assert len(visits_result) == len(visits_df)
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in visits_result["patient_id"]
        )
        # Check that values are sequential (continuing from where patients left off)
        visit_deid_values = sorted(visits_result["patient_id"].tolist())
        assert visit_deid_values == [
            1,
            2,
            6,
            7,
            8,
        ]  # 1,2 from overlap; 6,7,8 are new sequential values

        # Check that deid_ref_dict has all unique patient IDs
        assert "patient_id" in deid_ref_dict
        patient_deid_df = deid_ref_dict["patient_id"]
        assert len(patient_deid_df) == 8  # 5 from patients + 3 new from visits
        assert set(patient_deid_df["patient_id"]) == {1, 2, 3, 4, 5, 6, 7, 8}

        # Verify that overlapping IDs have same de-identified values
        # Get de-identified values for overlapping IDs
        id1_deid = patient_deid_df[patient_deid_df["patient_id"] == 1][
            "patient_id__deid"
        ].iloc[0]
        id2_deid = patient_deid_df[patient_deid_df["patient_id"] == 2][
            "patient_id__deid"
        ].iloc[0]

        # Check that overlapping IDs have same de-identified values in both results
        assert patients_result[patients_result["patient_id"] == id1_deid].shape[0] > 0
        assert visits_result[visits_result["patient_id"] == id1_deid].shape[0] > 0
        assert patients_result[patients_result["patient_id"] == id2_deid].shape[0] > 0
        assert visits_result[visits_result["patient_id"] == id2_deid].shape[0] > 0

    @patch("cleared.io.create_data_loader")
    def test_two_tables_same_uid_different_names(self, mock_create_loader):
        """Test IDDeidentifier with two tables having same UID but different column names."""
        # Create data with different column names but same UID
        patients_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            }
        )

        users_df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],  # Different name but same values
                "username": ["alice", "bob", "charlie", "diana", "eve"],
            }
        )

        # Setup mock data loader
        mock_loader = MockDataLoader({})
        mock_loader.data["patients"] = patients_df.copy()
        mock_loader.data["users"] = users_df.copy()
        mock_create_loader.return_value = mock_loader

        # Create IDDeidentifier transformers with same UID but different names
        patient_id_config = IdentifierConfig(
            name="patient_id",
            uid="global_user_id",  # Same UID
            description="Global user identifier",
        )
        patient_id_transformer = IDDeidentifier(
            patient_id_config, uid="patient_id_transformer"
        )

        user_id_config = IdentifierConfig(
            name="user_id",
            uid="global_user_id",  # Same UID
            description="Global user identifier",
        )
        user_id_transformer = IDDeidentifier(user_id_config, uid="user_id_transformer")

        # Create table pipelines
        patients_pipeline = TablePipeline(
            table_name="patients",
            io_config=self.io_config,
            deid_config=self.deid_config,
        )
        patients_pipeline.add_transformer(patient_id_transformer)

        users_pipeline = TablePipeline(
            table_name="users", io_config=self.io_config, deid_config=self.deid_config
        )
        users_pipeline.add_transformer(user_id_transformer)

        # Start with empty deid_ref_dict
        deid_ref_dict = {}

        # Transform both tables
        patients_result, deid_ref_dict = patients_pipeline.transform(
            deid_ref_dict=deid_ref_dict
        )
        users_result, deid_ref_dict = users_pipeline.transform(
            deid_ref_dict=deid_ref_dict
        )

        # Verify results
        assert len(patients_result) == len(patients_df)
        assert len(users_result) == len(users_df)

        # Check that both tables have de-identified values
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in patients_result["patient_id"]
        )
        # Check that values are sequential
        patient_deid_values = sorted(patients_result["patient_id"].tolist())
        assert patient_deid_values == [1, 2, 3, 4, 5]
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in users_result["user_id"]
        )
        # Check that values are sequential
        user_deid_values = sorted(users_result["user_id"].tolist())
        assert user_deid_values == [1, 2, 3, 4, 5]

        # Check that deid_ref_dict has the shared UID
        assert "global_user_id" in deid_ref_dict
        global_deid_df = deid_ref_dict["global_user_id"]

        # Should have all unique values from both tables
        assert len(global_deid_df) == 5  # Same values in both tables
        assert set(global_deid_df["global_user_id"]) == {1, 2, 3, 4, 5}

        # Verify that same original values have same de-identified values
        # This tests that the shared UID creates consistent mappings
        id1_deid = global_deid_df[global_deid_df["global_user_id"] == 1][
            "global_user_id__deid"
        ].iloc[0]
        assert patients_result[patients_result["patient_id"] == id1_deid].shape[0] > 0
        assert users_result[users_result["user_id"] == id1_deid].shape[0] > 0

    @patch("cleared.io.create_data_loader")
    def test_single_table_two_id_columns_different_uids(self, mock_create_loader):
        """Test IDDeidentifier with one table having two ID columns with different UIDs."""
        # Setup mock data loader
        mock_loader = MockDataLoader({})
        mock_loader.data["mixed"] = self.mixed_ids_df.copy()
        mock_create_loader.return_value = mock_loader

        # Create IDDeidentifier transformers for different ID columns
        patient_id_config = IdentifierConfig(
            name="patient_id", uid="patient_id", description="Patient identifier"
        )
        patient_id_transformer = IDDeidentifier(
            patient_id_config, uid="patient_id_transformer"
        )

        encounter_id_config = IdentifierConfig(
            name="encounter_id", uid="encounter_id", description="Encounter identifier"
        )
        encounter_id_transformer = IDDeidentifier(
            encounter_id_config, uid="encounter_id_transformer"
        )

        # Create table pipeline with both transformers
        pipeline = TablePipeline(
            table_name="mixed", io_config=self.io_config, deid_config=self.deid_config
        )
        pipeline.add_transformer(patient_id_transformer)
        pipeline.add_transformer(encounter_id_transformer)

        # Start with empty deid_ref_dict
        deid_ref_dict = {}

        # Transform the data
        result_df, result_deid_ref_dict = pipeline.transform(
            deid_ref_dict=deid_ref_dict
        )

        # Verify results
        assert len(result_df) == len(self.mixed_ids_df)
        assert "patient_id" in result_df.columns
        assert "encounter_id" in result_df.columns
        assert "name" in result_df.columns
        assert "diagnosis" in result_df.columns

        # Check that both ID columns are de-identified
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in result_df["patient_id"]
        )
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in result_df["encounter_id"]
        )
        # Check that values are sequential
        patient_deid_values = sorted(result_df["patient_id"].tolist())
        encounter_deid_values = sorted(result_df["encounter_id"].tolist())
        assert patient_deid_values == [1, 2, 3, 4, 5]
        assert encounter_deid_values == [
            1,
            2,
            3,
            4,
            5,
        ]  # Each transformer starts sequential numbering from 1

        # Check that deid_ref_dict has both UIDs
        assert "patient_id" in result_deid_ref_dict
        assert "encounter_id" in result_deid_ref_dict

        # Verify patient_id deid_ref_df
        patient_deid_df = result_deid_ref_dict["patient_id"]
        assert len(patient_deid_df) == 5
        assert set(patient_deid_df["patient_id"]) == {1, 2, 3, 4, 5}

        # Verify encounter_id deid_ref_df
        encounter_deid_df = result_deid_ref_dict["encounter_id"]
        assert len(encounter_deid_df) == 5
        assert set(encounter_deid_df["encounter_id"]) == {101, 102, 103, 104, 105}

    @patch("cleared.io.create_data_loader")
    def test_single_table_two_id_columns_same_uid_overlapping_values(
        self, mock_create_loader
    ):
        """Test IDDeidentifier with one table having two ID columns with same UID but different values."""
        # Create data with overlapping and non-overlapping values
        overlapping_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "encounter_id": [1, 2, 6, 7, 8],  # Overlap: 1, 2; New: 6, 7, 8
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "diagnosis": ["Cold", "Fever", "Headache", "Cough", "Flu"],
            }
        )

        # Setup mock data loader
        mock_loader = MockDataLoader({})
        mock_loader.data["overlapping"] = overlapping_df.copy()
        mock_create_loader.return_value = mock_loader

        # Create IDDeidentifier transformers with same UID but different names
        patient_id_config = IdentifierConfig(
            name="patient_id",
            uid="global_id",  # Same UID
            description="Global identifier",
        )
        patient_id_transformer = IDDeidentifier(
            patient_id_config, uid="patient_id_transformer"
        )

        encounter_id_config = IdentifierConfig(
            name="encounter_id",
            uid="global_id",  # Same UID
            description="Global identifier",
        )
        encounter_id_transformer = IDDeidentifier(
            encounter_id_config, uid="encounter_id_transformer"
        )

        # Create table pipeline with both transformers
        pipeline = TablePipeline(
            table_name="overlapping",
            io_config=self.io_config,
            deid_config=self.deid_config,
        )
        pipeline.add_transformer(patient_id_transformer)
        pipeline.add_transformer(encounter_id_transformer)

        # Start with empty deid_ref_dict
        deid_ref_dict = {}

        # Transform the data
        result_df, result_deid_ref_dict = pipeline.transform(
            deid_ref_dict=deid_ref_dict
        )

        # Verify results
        assert len(result_df) == len(overlapping_df)
        assert "patient_id" in result_df.columns
        assert "encounter_id" in result_df.columns

        # Check that both ID columns are de-identified
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in result_df["patient_id"]
        )
        assert all(
            isinstance(val, (int, np.integer, float)) and val == int(val)
            for val in result_df["encounter_id"]
        )
        # Check that values are sequential (shared UID means shared sequential numbering)
        patient_deid_values = sorted(result_df["patient_id"].tolist())
        encounter_deid_values = sorted(result_df["encounter_id"].tolist())
        assert patient_deid_values == [1, 2, 3, 4, 5]
        assert encounter_deid_values == [
            1,
            2,
            6,
            7,
            8,
        ]  # 1,2 from overlap; 6,7,8 are new sequential values

        # Check that deid_ref_dict has the shared UID
        assert "global_id" in result_deid_ref_dict
        global_deid_df = result_deid_ref_dict["global_id"]

        # Should have all unique values from both columns
        assert len(global_deid_df) == 8  # 5 from patient_id + 3 new from encounter_id
        assert set(global_deid_df["global_id"]) == {1, 2, 3, 4, 5, 6, 7, 8}

        # Verify that overlapping values have same de-identified values
        id1_deid = global_deid_df[global_deid_df["global_id"] == 1][
            "global_id__deid"
        ].iloc[0]
        id2_deid = global_deid_df[global_deid_df["global_id"] == 2][
            "global_id__deid"
        ].iloc[0]

        # Check that overlapping IDs have same de-identified values in both columns
        assert result_df[result_df["patient_id"] == id1_deid].shape[0] > 0
        assert result_df[result_df["encounter_id"] == id1_deid].shape[0] > 0
        assert result_df[result_df["patient_id"] == id2_deid].shape[0] > 0
        assert result_df[result_df["encounter_id"] == id2_deid].shape[0] > 0

        # Verify that non-overlapping values have different de-identified values
        id3_deid = global_deid_df[global_deid_df["global_id"] == 3][
            "global_id__deid"
        ].iloc[0]
        id6_deid = global_deid_df[global_deid_df["global_id"] == 6][
            "global_id__deid"
        ].iloc[0]

        assert id3_deid != id6_deid
        assert result_df[result_df["patient_id"] == id3_deid].shape[0] > 0
        assert result_df[result_df["encounter_id"] == id6_deid].shape[0] > 0
