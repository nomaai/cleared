"""Edge cases and error scenario tests for TransformerRegistry."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig
from cleared.transformers.registry import (
    TransformerRegistry,
)
from cleared.transformers.base import BaseTransformer
import pandas as pd


class TestTransformerRegistryEdgeCases:
    """Test edge cases and error scenarios for TransformerRegistry."""

    def setup_method(self):
        """Set up test fixtures."""

        class MockTransformer(BaseTransformer):
            def __init__(self, **kwargs):
                super().__init__()
                self.configs = kwargs

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

        self.MockTransformer = MockTransformer

    def test_empty_registry_operations(self):
        """Test operations on empty registry."""
        registry = TransformerRegistry(use_defaults=False)

        # Test listing available transformers
        assert registry.list_available() == []

        # Test getting registry info
        assert registry.get_registry_info() == {}

        # Test checking if transformer is registered
        assert not registry.is_registered("AnyTransformer")

        # Test length
        assert len(registry) == 0

        # Test contains
        assert "AnyTransformer" not in registry

        # Test representation
        assert repr(registry) == "TransformerRegistry(0 transformers: [])"

    def test_register_with_none_name(self):
        """Test registering with None as name."""
        registry = TransformerRegistry(use_defaults=False)

        # None should be allowed as a name (though not recommended)
        registry.register(None, self.MockTransformer)
        assert None in registry
        assert registry.get_class(None) == self.MockTransformer

    def test_register_with_very_long_name(self):
        """Test registering with very long name."""
        registry = TransformerRegistry(use_defaults=False)

        long_name = "A" * 1000  # 1000 character name
        registry.register(long_name, self.MockTransformer)
        assert long_name in registry

    def test_register_with_special_characters_in_name(self):
        """Test registering with special characters in name."""
        registry = TransformerRegistry(use_defaults=False)

        special_names = [
            "transformer-with-dashes",
            "transformer_with_underscores",
            "transformer.with.dots",
            "transformer123",
            "transformer@special",
            "transformer#hash",
            "transformer$dollar",
            "transformer%percent",
            "transformer^caret",
            "transformer&ampersand",
            "transformer*asterisk",
            "transformer(open_paren",
            "transformer)close_paren",
            "transformer+plus",
            "transformer=equals",
            "transformer[open_bracket",
            "transformer]close_bracket",
            "transformer{open_brace",
            "transformer}close_brace",
            "transformer|pipe",
            "transformer\\backslash",
            "transformer:colon",
            "transformer;semicolon",
            'transformer"double_quote',
            "transformer'single_quote",
            "transformer<less_than",
            "transformer,comma",
            "transformer>greater_than",
            "transformer?question",
            "transformer/slash",
            "transformer space",
            "transformer\ttab",
            "transformer\nnewline",
        ]

        for name in special_names:
            registry.register(name, self.MockTransformer)
            assert name in registry

    def test_instantiate_with_none_config(self):
        """Test instantiating with None config."""
        registry = TransformerRegistry(use_defaults=False)

        class NoneAwareTransformer(BaseTransformer):
            def __init__(self, **kwargs):
                super().__init__(global_deid_config=kwargs.get("global_deid_config"))
                self.configs = kwargs
                # Check if only global_deid_config (which may be None) is present
                non_global_keys = {
                    k: v for k, v in kwargs.items() if k != "global_deid_config"
                }
                self.is_none = len(non_global_keys) == 0

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

        registry.register("NoneAwareTransformer", NoneAwareTransformer)

        transformer = registry.instantiate("NoneAwareTransformer", None)
        assert transformer.is_none is True

    def test_instantiate_with_empty_dict_config(self):
        """Test instantiating with empty dict config."""
        registry = TransformerRegistry(use_defaults=False)

        class EmptyConfigTransformer(BaseTransformer):
            def __init__(self, **kwargs):
                super().__init__()
                self.configs = kwargs
                self.is_empty = len(kwargs) == 0

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

        registry.register("EmptyConfigTransformer", EmptyConfigTransformer)

        transformer = registry.instantiate("EmptyConfigTransformer", {})
        assert transformer.is_empty is True

    def test_instantiate_with_empty_dictconfig(self):
        """Test instantiating with empty DictConfig."""
        registry = TransformerRegistry(use_defaults=False)

        class EmptyDictConfigTransformer(BaseTransformer):
            def __init__(self, **kwargs):
                super().__init__()
                self.configs = kwargs
                self.is_empty = len(kwargs) == 0

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

        registry.register("EmptyDictConfigTransformer", EmptyDictConfigTransformer)

        empty_config = DictConfig({})
        transformer = registry.instantiate("EmptyDictConfigTransformer", empty_config)
        assert transformer.is_empty is True

    def test_register_duplicate_with_different_cases(self):
        """Test registering transformers with different cases."""
        registry = TransformerRegistry(use_defaults=False)

        registry.register("transformer", self.MockTransformer)
        registry.register("Transformer", self.MockTransformer)
        registry.register("TRANSFORMER", self.MockTransformer)

        # All should be registered as different names
        assert len(registry) == 3
        assert "transformer" in registry
        assert "Transformer" in registry
        assert "TRANSFORMER" in registry

    def test_unregister_all_transformers(self):
        """Test unregistering all transformers one by one."""
        registry = TransformerRegistry(use_defaults=False)

        # Register multiple transformers
        for i in range(5):
            registry.register(f"Transformer{i}", self.MockTransformer)

        assert len(registry) == 5

        # Unregister all
        for i in range(5):
            registry.unregister(f"Transformer{i}")

        assert len(registry) == 0
        assert registry.list_available() == []

    def test_register_with_unicode_names(self):
        """Test registering with unicode names."""
        registry = TransformerRegistry(use_defaults=False)

        unicode_names = [
            "transformer_a",  # Greek alpha
            "transformer_β",  # Greek beta
            "transformer_中文",  # Chinese characters
            "transformer_日本語",  # Japanese characters
            "transformer_한국어",  # Korean characters
            "transformer_العربية",  # Arabic characters
            "transformer_русский",  # Cyrillic characters
            "transformer_עברית",  # Hebrew characters
            "transformer_हिन्दी",  # Devanagari characters
            "transformer_🌍",  # Emoji
            "transformer_🚀",  # Emoji
        ]

        for name in unicode_names:
            registry.register(name, self.MockTransformer)
            assert name in registry

    def test_instantiate_with_complex_nested_config(self):
        """Test instantiating with complex nested configuration."""
        registry = TransformerRegistry(use_defaults=False)

        class ComplexConfigTransformer(BaseTransformer):
            def __init__(
                self,
                nested: dict | None = None,
                list_value: list | None = None,
                boolean: bool = False,
                **kwargs,
            ):
                super().__init__()
                self.configs = kwargs
                self.nested_value = (
                    nested.get("value", "default") if nested else "default"
                )
                self.list_value = list_value or []
                self.bool_value = boolean

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

        registry.register("ComplexConfigTransformer", ComplexConfigTransformer)

        complex_config = DictConfig(
            {
                "nested": {
                    "value": "nested_value",
                    "sub_nested": {"deep_value": "deep"},
                },
                "list_value": [1, 2, 3, "string", True],
                "boolean": True,
                "string": "test",
            }
        )

        transformer = registry.instantiate("ComplexConfigTransformer", complex_config)
        assert transformer.nested_value == "nested_value"
        assert transformer.list_value == [1, 2, 3, "string", True]
        assert transformer.bool_value is True

    def test_registry_with_transformer_that_raises_in_init(self):
        """Test registry with transformer that raises exception in __init__."""
        registry = TransformerRegistry(use_defaults=False)

        class FailingTransformer(BaseTransformer):
            def __init__(self, **kwargs):
                super().__init__()
                raise RuntimeError("Simulated initialization failure")

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

        registry.register("FailingTransformer", FailingTransformer)

        config = DictConfig({"test": "value"})

        with pytest.raises(TypeError) as exc_info:
            registry.instantiate("FailingTransformer", config)

        assert "Failed to create transformer" in str(exc_info.value)
        assert "Simulated initialization failure" in str(exc_info.value)

    def test_registry_with_transformer_that_has_no_transform_method(self):
        """Test registry with transformer that doesn't implement transform method."""
        registry = TransformerRegistry(use_defaults=False)

        class IncompleteTransformer(BaseTransformer):
            def __init__(self, **kwargs):
                super().__init__()
                self.configs = kwargs

            # Missing transform method

        # This should still be allowed to register since it inherits from BaseTransformer
        registry.register("IncompleteTransformer", IncompleteTransformer)
        assert "IncompleteTransformer" in registry

        # But instantiation should fail because it's an abstract class
        config = DictConfig({"test": "value"})
        with pytest.raises(TypeError) as exc_info:
            registry.instantiate("IncompleteTransformer", config)

        assert "Failed to create transformer" in str(exc_info.value)
        assert "abstract method" in str(exc_info.value) and "transform" in str(
            exc_info.value
        )

    def test_registry_with_very_large_number_of_transformers(self):
        """Test registry with a very large number of transformers."""
        registry = TransformerRegistry(use_defaults=False)

        # Register many transformers
        num_transformers = 1000
        for i in range(num_transformers):
            registry.register(f"Transformer{i:04d}", self.MockTransformer)

        assert len(registry) == num_transformers

        # Test that we can still access them
        assert "Transformer0000" in registry
        assert "Transformer0999" in registry

        # Test instantiation
        config = DictConfig({"test": "value"})
        transformer = registry.instantiate("Transformer0500", config)
        assert isinstance(transformer, self.MockTransformer)

    def test_registry_clear_and_rebuild_large(self):
        """Test clearing and rebuilding a large registry."""
        registry = TransformerRegistry(use_defaults=False)

        # Build large registry
        for i in range(100):
            registry.register(f"Transformer{i}", self.MockTransformer)

        assert len(registry) == 100

        # Clear it
        registry.clear()
        assert len(registry) == 0

        # Rebuild it
        for i in range(50):
            registry.register(f"NewTransformer{i}", self.MockTransformer)

        assert len(registry) == 50
        assert "NewTransformer0" in registry
        assert "NewTransformer49" in registry

    def test_registry_with_duplicate_class_registration(self):
        """Test registering the same class under different names."""
        registry = TransformerRegistry(use_defaults=False)

        # Register the same class under different names
        registry.register("Transformer1", self.MockTransformer)
        registry.register("Transformer2", self.MockTransformer)
        registry.register("Transformer3", self.MockTransformer)

        assert len(registry) == 3

        # All should point to the same class
        assert registry.get_class("Transformer1") == self.MockTransformer
        assert registry.get_class("Transformer2") == self.MockTransformer
        assert registry.get_class("Transformer3") == self.MockTransformer

    def test_registry_error_messages_with_many_transformers(self):
        """Test error messages when registry has many transformers."""
        registry = TransformerRegistry(use_defaults=False)

        # Register many transformers
        for i in range(20):
            registry.register(f"Transformer{i}", self.MockTransformer)

        with pytest.raises(KeyError) as exc_info:
            registry.instantiate("NonExistentTransformer", DictConfig({}))

        error_msg = str(exc_info.value)
        assert "Unknown transformer 'NonExistentTransformer'" in error_msg
        assert "Available transformers:" in error_msg

        # Should list all available transformers
        for i in range(20):
            assert f"Transformer{i}" in error_msg

    def test_registry_with_transformer_that_modifies_self(self):
        """Test registry with transformer that modifies itself during instantiation."""
        registry = TransformerRegistry(use_defaults=False)

        class SelfModifyingTransformer(BaseTransformer):
            def __init__(self, value: int):
                super().__init__()
                # Modify self during initialization
                self.modified = True
                self.value = value * 2

            def transform(
                self, df: pd.DataFrame, deid_ref_dict: dict[str, pd.DataFrame]
            ):
                return df.copy(), deid_ref_dict.copy()

        registry.register("SelfModifyingTransformer", SelfModifyingTransformer)

        config = DictConfig({"value": 5})
        transformer = registry.instantiate("SelfModifyingTransformer", config)

        assert transformer.modified is True
        assert transformer.value == 10  # 5 * 2
