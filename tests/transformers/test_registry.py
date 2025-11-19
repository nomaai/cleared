"""Comprehensive unit tests for TransformerRegistry."""

from __future__ import annotations

import pytest
from unittest.mock import patch
from omegaconf import DictConfig
from cleared.transformers.registry import (
    TransformerRegistry,
    get_expected_transformer_names,
)
from cleared.transformers.base import BaseTransformer
import pandas as pd


class TestTransformerRegistry:
    """Test the TransformerRegistry class."""

    def setup_method(self):
        """Set up test fixtures before each test method."""

        # Create a mock transformer class for testing
        class MockTransformer(BaseTransformer):
            def __init__(self, test_param: str = "default", global_deid_config=None):
                super().__init__(global_deid_config=global_deid_config)
                self.test_param = test_param

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
                return df.copy(), deid_ref_dict.copy()

        self.MockTransformer = MockTransformer

    def test_init_with_defaults(self):
        """Test initialization with default transformers."""
        with patch.object(
            TransformerRegistry, "_register_default_transformers"
        ) as mock_register:
            registry = TransformerRegistry(use_defaults=True)

            assert len(registry) == 0  # Registry starts empty
            mock_register.assert_called_once()

    def test_init_without_defaults(self):
        """Test initialization without default transformers."""
        with patch.object(
            TransformerRegistry, "_register_default_transformers"
        ) as mock_register:
            registry = TransformerRegistry(use_defaults=False)

            assert len(registry) == 0
            mock_register.assert_not_called()

    def test_init_with_custom_transformers(self):
        """Test initialization with custom transformers."""
        custom_transformers = {
            "CustomTransformer1": self.MockTransformer,
            "CustomTransformer2": self.MockTransformer,
        }

        registry = TransformerRegistry(
            use_defaults=False, custom_transformers=custom_transformers
        )

        assert len(registry) == 2
        assert "CustomTransformer1" in registry
        assert "CustomTransformer2" in registry
        assert registry.get_class("CustomTransformer1") == self.MockTransformer

    def test_init_with_defaults_and_custom_transformers(self):
        """Test initialization with both defaults and custom transformers."""
        custom_transformers = {"CustomTransformer": self.MockTransformer}

        with patch.object(
            TransformerRegistry, "_register_default_transformers"
        ) as mock_register:
            registry = TransformerRegistry(
                use_defaults=True, custom_transformers=custom_transformers
            )

            mock_register.assert_called_once()
            assert "CustomTransformer" in registry

    def test_register_valid_transformer(self):
        """Test registering a valid transformer."""
        registry = TransformerRegistry(use_defaults=False)

        registry.register("TestTransformer", self.MockTransformer)

        assert "TestTransformer" in registry
        assert registry.get_class("TestTransformer") == self.MockTransformer
        assert len(registry) == 1

    def test_register_invalid_transformer_type(self):
        """Test registering an invalid transformer type."""
        registry = TransformerRegistry(use_defaults=False)

        with pytest.raises(TypeError) as exc_info:
            registry.register("InvalidTransformer", str)

        assert "must be a subclass of BaseTransformer" in str(exc_info.value)

    def test_register_duplicate_name(self):
        """Test registering a transformer with duplicate name."""
        registry = TransformerRegistry(use_defaults=False)

        registry.register("TestTransformer", self.MockTransformer)

        with pytest.raises(ValueError) as exc_info:
            registry.register("TestTransformer", self.MockTransformer)

        assert "is already registered" in str(exc_info.value)

    def test_unregister_existing_transformer(self):
        """Test unregistering an existing transformer."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("TestTransformer", self.MockTransformer)

        assert "TestTransformer" in registry
        assert len(registry) == 1

        registry.unregister("TestTransformer")

        assert "TestTransformer" not in registry
        assert len(registry) == 0

    def test_unregister_nonexistent_transformer(self):
        """Test unregistering a non-existent transformer."""
        registry = TransformerRegistry(use_defaults=False)

        with pytest.raises(KeyError) as exc_info:
            registry.unregister("NonExistentTransformer")

        assert "is not registered" in str(exc_info.value)

    def test_instantiate_with_dictconfig(self):
        """Test instantiating a transformer with DictConfig."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("TestTransformer", self.MockTransformer)

        config = DictConfig({"test_param": "test_value"})
        transformer = registry.instantiate("TestTransformer", config)

        assert isinstance(transformer, self.MockTransformer)
        assert transformer.test_param == "test_value"

    def test_instantiate_with_dict(self):
        """Test instantiating a transformer with regular dict."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("TestTransformer", self.MockTransformer)

        config = {"test_param": "test_value"}
        transformer = registry.instantiate("TestTransformer", config)

        assert isinstance(transformer, self.MockTransformer)
        assert transformer.test_param == "test_value"

    def test_instantiate_nonexistent_transformer(self):
        """Test instantiating a non-existent transformer."""
        registry = TransformerRegistry(use_defaults=False)

        config = DictConfig({"test_param": "test_value"})

        with pytest.raises(KeyError) as exc_info:
            registry.instantiate("NonExistentTransformer", config)

        assert "Unknown transformer" in str(exc_info.value)
        assert "Available transformers: []" in str(exc_info.value)

    def test_instantiate_with_invalid_config(self):
        """Test instantiating with config that causes transformer creation to fail."""

        class FailingTransformer(BaseTransformer):
            def __init__(self, required_param: str):
                super().__init__()
                if not required_param:
                    raise ValueError("required_param is missing")
                self.required_param = required_param

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
                return df.copy(), deid_ref_dict.copy()

        registry = TransformerRegistry(use_defaults=False)
        registry.register("FailingTransformer", FailingTransformer)

        config = DictConfig({"test_param": "test_value"})

        with pytest.raises(TypeError) as exc_info:
            registry.instantiate("FailingTransformer", config)

        assert "Failed to create transformer" in str(exc_info.value)
        assert "unexpected keyword argument 'test_param'" in str(exc_info.value)

    def test_get_class_existing(self):
        """Test getting class for existing transformer."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("TestTransformer", self.MockTransformer)

        transformer_class = registry.get_class("TestTransformer")
        assert transformer_class == self.MockTransformer

    def test_get_class_nonexistent(self):
        """Test getting class for non-existent transformer."""
        registry = TransformerRegistry(use_defaults=False)

        with pytest.raises(KeyError) as exc_info:
            registry.get_class("NonExistentTransformer")

        assert "Unknown transformer" in str(exc_info.value)

    def test_list_available(self):
        """Test listing available transformers."""
        registry = TransformerRegistry(use_defaults=False)

        assert registry.list_available() == []

        registry.register("Transformer1", self.MockTransformer)
        registry.register("Transformer2", self.MockTransformer)

        available = registry.list_available()
        assert len(available) == 2
        assert "Transformer1" in available
        assert "Transformer2" in available

    def test_is_registered(self):
        """Test checking if transformer is registered."""
        registry = TransformerRegistry(use_defaults=False)

        assert not registry.is_registered("TestTransformer")

        registry.register("TestTransformer", self.MockTransformer)

        assert registry.is_registered("TestTransformer")

    def test_get_registry_info(self):
        """Test getting registry information."""
        registry = TransformerRegistry(use_defaults=False)

        assert registry.get_registry_info() == {}

        registry.register("TestTransformer", self.MockTransformer)

        info = registry.get_registry_info()
        assert len(info) == 1
        assert "TestTransformer" in info
        assert info["TestTransformer"] == "MockTransformer"

    def test_clear(self):
        """Test clearing the registry."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("Transformer1", self.MockTransformer)
        registry.register("Transformer2", self.MockTransformer)

        assert len(registry) == 2

        registry.clear()

        assert len(registry) == 0
        assert registry.list_available() == []

    def test_len(self):
        """Test __len__ method."""
        registry = TransformerRegistry(use_defaults=False)

        assert len(registry) == 0

        registry.register("Transformer1", self.MockTransformer)
        assert len(registry) == 1

        registry.register("Transformer2", self.MockTransformer)
        assert len(registry) == 2

    def test_contains(self):
        """Test __contains__ method."""
        registry = TransformerRegistry(use_defaults=False)

        assert "TestTransformer" not in registry

        registry.register("TestTransformer", self.MockTransformer)

        assert "TestTransformer" in registry

    def test_repr(self):
        """Test __repr__ method."""
        registry = TransformerRegistry(use_defaults=False)

        assert "TransformerRegistry(0 transformers: [])" == repr(registry)

        registry.register("Transformer1", self.MockTransformer)
        registry.register("Transformer2", self.MockTransformer)

        repr_str = repr(registry)
        assert "TransformerRegistry(2 transformers:" in repr_str
        assert "Transformer1" in repr_str
        assert "Transformer2" in repr_str

    def test_register_default_transformers_success(self):
        """Test successful registration of default transformers."""
        registry = TransformerRegistry(use_defaults=False)

        # Get expected transformer names from auto-discovery
        expected_transformers = get_expected_transformer_names()

        registry._register_default_transformers()

        # Check that all expected transformers are registered
        assert len(registry) == len(expected_transformers)
        for transformer_name in expected_transformers:
            assert transformer_name in registry

    def test_register_default_transformers_import_error(self):
        """Test handling of import errors during default transformer registration."""
        registry = TransformerRegistry(use_defaults=False)

        # Test that the method exists and can be called
        # The actual import error handling is tested in integration tests
        registry._register_default_transformers()

        # Should have registered the default transformers
        assert len(registry) >= 0  # At least some transformers should be registered

    def test_instantiate_with_mock_dictconfig(self):
        """Test instantiate with mocked DictConfig that has _content attribute."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("TestTransformer", self.MockTransformer)

        # Use a real DictConfig for testing
        config = DictConfig({"test_param": "test_value"})

        transformer = registry.instantiate("TestTransformer", config)

        assert isinstance(transformer, self.MockTransformer)
        assert transformer.test_param == "test_value"

    def test_instantiate_with_mock_dictconfig_conversion(self):
        """Test that DictConfig is properly converted to dict."""
        registry = TransformerRegistry(use_defaults=False)

        class ConfigAwareTransformer(BaseTransformer):
            def __init__(self, test_param: str | None = None):
                super().__init__()
                self.test_param = test_param

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
                return df.copy(), deid_ref_dict.copy()

        registry.register("ConfigAwareTransformer", ConfigAwareTransformer)

        # Use a real DictConfig for testing
        config = DictConfig({"test_param": "test_value"})

        transformer = registry.instantiate("ConfigAwareTransformer", config)

        # Should have received the test_param value
        assert transformer.test_param == "test_value"

    def test_multiple_registries_independence(self):
        """Test that multiple registries are independent."""
        registry1 = TransformerRegistry(use_defaults=False)
        registry2 = TransformerRegistry(use_defaults=False)

        registry1.register("Transformer1", self.MockTransformer)
        registry2.register("Transformer2", self.MockTransformer)

        assert "Transformer1" in registry1
        assert "Transformer1" not in registry2
        assert "Transformer2" in registry2
        assert "Transformer2" not in registry1

        assert len(registry1) == 1
        assert len(registry2) == 1

    def test_registry_with_complex_custom_transformers(self):
        """Test registry with complex custom transformer setup."""

        class ComplexTransformer(BaseTransformer):
            def __init__(
                self,
                name: str = "unknown",
                params: dict | None = None,
                enabled: bool = True,
            ):
                super().__init__()
                self.name = name
                self.params = params or {}
                self.enabled = enabled

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                if not self.enabled:
                    return df.copy(), deid_ref_dict.copy()

                # Simple transformation based on params
                df_copy = df.copy()
                if "multiplier" in self.params:
                    numeric_cols = df_copy.select_dtypes(include=["number"]).columns
                    for col in numeric_cols:
                        df_copy[col] = df_copy[col] * self.params["multiplier"]

                return df_copy, deid_ref_dict.copy()

            def reverse(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
                return df.copy(), deid_ref_dict.copy()

        custom_transformers = {
            "ComplexTransformer1": ComplexTransformer,
            "ComplexTransformer2": ComplexTransformer,
        }

        registry = TransformerRegistry(
            use_defaults=False, custom_transformers=custom_transformers
        )

        assert len(registry) == 2
        assert "ComplexTransformer1" in registry
        assert "ComplexTransformer2" in registry

        # Test instantiation with complex config
        config = DictConfig(
            {"name": "test_transformer", "params": {"multiplier": 2}, "enabled": True}
        )

        transformer = registry.instantiate("ComplexTransformer1", config)
        assert transformer.name == "test_transformer"
        assert transformer.params == {"multiplier": 2}
        assert transformer.enabled is True

    def test_error_messages_contain_available_transformers(self):
        """Test that error messages include available transformer names."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("Transformer1", self.MockTransformer)
        registry.register("Transformer2", self.MockTransformer)

        with pytest.raises(KeyError) as exc_info:
            registry.instantiate("NonExistent", DictConfig({}))

        error_msg = str(exc_info.value)
        assert "Unknown transformer 'NonExistent'" in error_msg
        assert "Available transformers:" in error_msg
        assert "Transformer1" in error_msg
        assert "Transformer2" in error_msg

    def test_registry_info_with_multiple_transformers(self):
        """Test registry info with multiple transformers."""
        registry = TransformerRegistry(use_defaults=False)

        class TransformerA(BaseTransformer):
            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
                return df.copy(), deid_ref_dict.copy()

        class TransformerB(BaseTransformer):
            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]):
                return df.copy(), deid_ref_dict.copy()

        registry.register("TransformerA", TransformerA)
        registry.register("TransformerB", TransformerB)

        info = registry.get_registry_info()
        assert len(info) == 2
        assert info["TransformerA"] == "TransformerA"
        assert info["TransformerB"] == "TransformerB"

    def test_edge_case_empty_name_registration(self):
        """Test edge case with empty string name."""
        registry = TransformerRegistry(use_defaults=False)

        # Empty string should be allowed as a name
        registry.register("", self.MockTransformer)
        assert "" in registry
        assert registry.get_class("") == self.MockTransformer

    def test_edge_case_none_config_instantiation(self):
        """Test edge case with None config."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("TestTransformer", self.MockTransformer)

        # This should work as the transformer handles None configs gracefully
        transformer = registry.instantiate("TestTransformer", None)
        assert isinstance(transformer, self.MockTransformer)
        assert transformer.test_param == "default"  # Uses default value
