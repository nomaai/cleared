"""Integration tests for TransformerRegistry with real transformers."""

import pytest
import pandas as pd
from omegaconf import DictConfig
from cleared.transformers.registry import (
    TransformerRegistry,
    get_expected_transformer_names,
)
from cleared.transformers.base import BaseTransformer


class TestTransformerRegistryIntegration:
    """Integration tests using real transformer classes."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create test data
        self.test_df = pd.DataFrame(
            {
                "patient_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "admission_date": [
                    "2023-01-15",
                    "2023-02-20",
                    "2023-03-10",
                    "2023-04-05",
                    "2023-05-12",
                ],
            }
        )

        self.test_deid_ref_dict = {
            "test_transformer": pd.DataFrame(
                {"patient_id": [1, 2, 3, 4, 5], "patient_id__deid": [1, 2, 3, 4, 5]}
            )
        }

    def test_registry_with_real_id_transformer(self):
        """Test registry with real IDDeidentifier transformer."""
        registry = TransformerRegistry(use_defaults=True)

        # Check that IDDeidentifier is registered
        assert "IDDeidentifier" in registry
        assert registry.is_registered("IDDeidentifier")

        # Test instantiation
        from cleared.config.structure import IdentifierConfig

        idconfig = IdentifierConfig(
            name="patient_id", uid="patient_id", description="Patient identifier"
        )
        # Convert dataclass to dict for OmegaConf compatibility
        config = DictConfig(
            {
                "idconfig": {
                    "name": idconfig.name,
                    "uid": idconfig.uid,
                    "description": idconfig.description,
                }
            }
        )
        transformer = registry.instantiate("IDDeidentifier", config)

        assert transformer.idconfig.name == "patient_id"

        # Test actual transformation
        result_df, _ = transformer.transform(self.test_df, self.test_deid_ref_dict)

        # Verify the transformation worked
        assert len(result_df) == len(self.test_df)
        assert "patient_id" in result_df.columns
        # The patient_id column should be replaced with de-identified values
        # Check that all values are sequential integers
        assert all(
            isinstance(val, (int, float)) and val == int(val)
            for val in result_df["patient_id"]
        )
        # Check that values are sequential starting from 1
        deid_values = sorted(result_df["patient_id"].tolist())
        assert deid_values == [1, 2, 3, 4, 5]

    def test_registry_with_real_temporal_transformer(self):
        """Test registry with real DateTimeDeidentifier transformer."""
        registry = TransformerRegistry(use_defaults=True)

        # Check that DateTimeDeidentifier is registered
        assert "DateTimeDeidentifier" in registry

        # Test instantiation with new config structure
        from cleared.config.structure import DeIDConfig, TimeShiftConfig

        config = DictConfig(
            {
                "idconfig": {
                    "name": "patient_id",
                    "uid": "patient_id",
                    "description": "Patient identifier",
                },
                "datetime_column": "admission_date",
            }
        )
        # Create global_deid_config separately
        time_shift_config = TimeShiftConfig(method="shift_by_days", min=1, max=30)
        global_deid_config = DeIDConfig(time_shift=time_shift_config)
        transformer = registry.instantiate(
            "DateTimeDeidentifier", config, global_deid_config=global_deid_config
        )

        assert transformer.datetime_column == "admission_date"
        assert transformer.idconfig.name == "patient_id"

    def test_registry_with_pipeline_transformers(self):
        """Test registry with pipeline transformers."""
        registry = TransformerRegistry(use_defaults=True)

        # Check that pipeline transformers are registered
        assert "TablePipeline" in registry

        # Test getting classes
        table_pipeline_class = registry.get_class("TablePipeline")

        assert table_pipeline_class is not None

    def test_registry_list_available_with_defaults(self):
        """Test listing available transformers with defaults."""
        registry = TransformerRegistry(use_defaults=True)

        available = registry.list_available()

        # Should contain the auto-discovered transformers
        expected_transformers = get_expected_transformer_names()
        for transformer in expected_transformers:
            assert transformer in available

    def test_registry_info_with_defaults(self):
        """Test registry info with default transformers."""
        registry = TransformerRegistry(use_defaults=True)

        info = registry.get_registry_info()

        # Should contain info about auto-discovered transformers
        expected_transformers = get_expected_transformer_names()
        for transformer in expected_transformers:
            assert transformer in info
            assert info[transformer] == transformer

    def test_mixed_default_and_custom_transformers(self):
        """Test registry with both default and custom transformers."""

        class CustomTransformer(BaseTransformer):
            def __init__(self, custom_param: str):
                super().__init__()
                self.custom_param = custom_param

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
                return df.copy(), deid_ref_dict.copy()

            def compare(
                self,
                original_df: pd.DataFrame,
                reversed_df: pd.DataFrame,
                deid_ref_dict: dict[str, pd.DataFrame] | None = None,
            ) -> list:
                """Mock compare method."""
                from cleared.models.verify_models import ColumnComparisonResult

                return [
                    ColumnComparisonResult(
                        column_name="mock_column",
                        status="pass",
                        message="Mock transformer comparison passed",
                        original_length=len(original_df),
                        reversed_length=len(reversed_df),
                        mismatch_count=0,
                        mismatch_percentage=0.0,
                    )
                ]

        custom_transformers = {"CustomTransformer": CustomTransformer}
        registry = TransformerRegistry(
            use_defaults=True, custom_transformers=custom_transformers
        )

        # Should have both default and custom transformers
        available = registry.list_available()

        # Check auto-discovered transformers are present
        expected_transformers = get_expected_transformer_names()
        for transformer in expected_transformers:
            assert transformer in available

        # Check custom transformer is present
        assert "CustomTransformer" in available

        # Test instantiation of custom transformer
        config = DictConfig({"custom_param": "test_value"})
        transformer = registry.instantiate("CustomTransformer", config)
        assert transformer.custom_param == "test_value"

    def test_registry_clear_and_rebuild(self):
        """Test clearing registry and rebuilding it."""
        registry = TransformerRegistry(use_defaults=True)

        # Should have default transformers
        initial_count = len(registry)
        assert initial_count > 0

        # Clear the registry
        registry.clear()
        assert len(registry) == 0
        assert registry.list_available() == []

        # Add custom transformer
        class TestTransformer(BaseTransformer):
            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
                return df.copy(), deid_ref_dict.copy()

            def compare(
                self,
                original_df: pd.DataFrame,
                reversed_df: pd.DataFrame,
                deid_ref_dict: dict[str, pd.DataFrame] | None = None,
            ) -> list:
                """Mock compare method."""
                from cleared.models.verify_models import ColumnComparisonResult

                return [
                    ColumnComparisonResult(
                        column_name="mock_column",
                        status="pass",
                        message="Mock transformer comparison passed",
                        original_length=len(original_df),
                        reversed_length=len(reversed_df),
                        mismatch_count=0,
                        mismatch_percentage=0.0,
                    )
                ]

        registry.register("TestTransformer", TestTransformer)
        assert len(registry) == 1
        assert "TestTransformer" in registry

    def test_registry_with_complex_configs(self):
        """Test registry with complex configuration objects."""
        registry = TransformerRegistry(use_defaults=True)

        # Test with nested DictConfig - only pass valid parameters
        from cleared.config.structure import IdentifierConfig

        idconfig = IdentifierConfig(
            name="patient_id",
            uid="patient_id",
            description="Patient identifier",
        )
        complex_config = DictConfig(
            {
                "idconfig": {
                    "name": idconfig.name,
                    "uid": idconfig.uid,
                    "description": idconfig.description,
                },
                "uid": "complex_transformer",
                "dependencies": ["dep1", "dep2"],
            }
        )

        transformer = registry.instantiate("IDDeidentifier", complex_config)
        assert transformer.idconfig.name == "patient_id"

    def test_registry_error_handling_with_real_transformers(self):
        """Test error handling with real transformers."""
        registry = TransformerRegistry(use_defaults=True)

        # Test with missing required parameter
        config = DictConfig({})  # Missing required 'idconfig' parameter

        with pytest.raises(TypeError) as exc_info:
            registry.instantiate("IDDeidentifier", config)

        assert "Failed to create transformer" in str(exc_info.value)

    def test_registry_performance_with_multiple_registrations(self):
        """Test registry performance with multiple registrations."""
        registry = TransformerRegistry(use_defaults=False)

        class TestTransformer(BaseTransformer):
            def __init__(self, id_value: int):
                super().__init__()
                self.id_value = id_value

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
                return df.copy(), deid_ref_dict.copy()

            def compare(
                self,
                original_df: pd.DataFrame,
                reversed_df: pd.DataFrame,
                deid_ref_dict: dict[str, pd.DataFrame] | None = None,
            ) -> list:
                """Mock compare method."""
                from cleared.models.verify_models import ColumnComparisonResult

                return [
                    ColumnComparisonResult(
                        column_name="mock_column",
                        status="pass",
                        message="Mock transformer comparison passed",
                        original_length=len(original_df),
                        reversed_length=len(reversed_df),
                        mismatch_count=0,
                        mismatch_percentage=0.0,
                    )
                ]

        # Register many transformers
        for i in range(100):
            registry.register(f"Transformer{i}", TestTransformer)

        assert len(registry) == 100

        # Test that we can still instantiate them
        config = DictConfig({"id_value": 42})
        transformer = registry.instantiate("Transformer50", config)
        assert transformer.id_value == 42

    def test_registry_with_inheritance(self):
        """Test registry with transformer inheritance."""

        class BaseCustomTransformer(BaseTransformer):
            def __init__(self, base_param: str, derived_param: str):
                super().__init__()
                self.base_param = base_param
                self.derived_param = derived_param

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
                return df.copy(), deid_ref_dict.copy()

            def compare(
                self,
                original_df: pd.DataFrame,
                reversed_df: pd.DataFrame,
                deid_ref_dict: dict[str, pd.DataFrame] | None = None,
            ) -> list:
                """Mock compare method."""
                from cleared.models.verify_models import ColumnComparisonResult

                return [
                    ColumnComparisonResult(
                        column_name="mock_column",
                        status="pass",
                        message="Mock transformer comparison passed",
                        original_length=len(original_df),
                        reversed_length=len(reversed_df),
                        mismatch_count=0,
                        mismatch_percentage=0.0,
                    )
                ]

        class DerivedTransformer(BaseCustomTransformer):
            def __init__(self, base_param: str, derived_param: str):
                super().__init__(base_param, derived_param)

        registry = TransformerRegistry(use_defaults=False)
        registry.register("DerivedTransformer", DerivedTransformer)

        config = DictConfig(
            {"base_param": "base_value", "derived_param": "derived_value"}
        )

        transformer = registry.instantiate("DerivedTransformer", config)
        assert transformer.base_param == "base_value"
        assert transformer.derived_param == "derived_value"
        assert isinstance(transformer, BaseCustomTransformer)
        assert isinstance(transformer, BaseTransformer)

    def test_registry_with_abstract_base_class_validation(self):
        """Test that registry properly validates BaseTransformer inheritance."""
        registry = TransformerRegistry(use_defaults=False)

        # Test with class that doesn't inherit from BaseTransformer
        class NotATransformer:
            def __init__(self, configs: dict):
                pass

        with pytest.raises(TypeError) as exc_info:
            registry.register("NotATransformer", NotATransformer)

        assert "must be a subclass of BaseTransformer" in str(exc_info.value)

    def test_registry_representation_with_real_transformers(self):
        """Test registry representation with real transformers."""
        registry = TransformerRegistry(use_defaults=True)

        repr_str = repr(registry)

        # Should contain the registry class name and transformer count
        assert "TransformerRegistry" in repr_str
        assert "transformers:" in repr_str

        # Should contain some of the default transformer names
        assert any(
            name in repr_str
            for name in [
                "IDDeidentifier",
                "DateTimeDeidentifier",
                "TablePipeline",
                "GlobalPipeline",
            ]
        )
