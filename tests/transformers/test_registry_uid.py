"""Tests for uid parameter handling in TransformerRegistry.instantiate()."""

from omegaconf import DictConfig

from cleared.transformers.registry import TransformerRegistry
from cleared.transformers.base import BaseTransformer
from cleared.cli.cmds.verify.model import ColumnComparisonResult


class TestRegistryUIDHandling:
    """Test that uid parameter is correctly passed to transformers."""

    def setup_method(self):
        """Set up test fixtures."""

        # Create a transformer that accepts uid
        class TransformerWithUID(BaseTransformer):
            def __init__(self, test_param: str, uid: str | None = None):
                super().__init__(uid=uid)
                self.test_param = test_param

            def transform(self, df, deid_ref_dict: dict):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df, deid_ref_dict: dict):
                return df.copy(), deid_ref_dict.copy()

            def compare(
                self,
                original_df,
                reversed_df,
                deid_ref_dict: dict | None = None,
            ) -> list:
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

        # Create a transformer that does NOT accept uid
        class TransformerWithoutUID(BaseTransformer):
            def __init__(self, test_param: str):
                super().__init__()  # No uid parameter
                self.test_param = test_param

            def transform(self, df, deid_ref_dict: dict):
                return df.copy(), deid_ref_dict.copy()

            def reverse(self, df, deid_ref_dict: dict):
                return df.copy(), deid_ref_dict.copy()

            def compare(
                self,
                original_df,
                reversed_df,
                deid_ref_dict: dict | None = None,
            ) -> list:
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

        self.TransformerWithUID = TransformerWithUID
        self.TransformerWithoutUID = TransformerWithoutUID

    def test_uid_passed_to_transformer_that_accepts_it(self):
        """Test that uid is correctly passed when transformer accepts it."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("TransformerWithUID", self.TransformerWithUID)

        config = DictConfig({"test_param": "test_value"})
        transformer = registry.instantiate(
            "TransformerWithUID", config, uid="custom_uid_123"
        )

        assert transformer.test_param == "test_value"
        assert transformer.uid == "custom_uid_123"

    def test_uid_not_passed_to_transformer_that_doesnt_accept_it(self):
        """Test that uid is silently ignored when transformer doesn't accept it."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("TransformerWithoutUID", self.TransformerWithoutUID)

        config = DictConfig({"test_param": "test_value"})
        # Should not raise an error even though uid is provided
        transformer = registry.instantiate(
            "TransformerWithoutUID", config, uid="custom_uid_123"
        )

        assert transformer.test_param == "test_value"
        # Transformer should have auto-generated uid, not the one we passed
        assert transformer.uid is not None
        assert transformer.uid != "custom_uid_123"

    def test_uid_none_when_not_provided(self):
        """Test that uid=None doesn't cause issues."""
        registry = TransformerRegistry(use_defaults=False)
        registry.register("TransformerWithUID", self.TransformerWithUID)

        config = DictConfig({"test_param": "test_value"})
        transformer = registry.instantiate("TransformerWithUID", config, uid=None)

        assert transformer.test_param == "test_value"
        # BaseTransformer auto-generates uid when None is provided
        assert transformer.uid is not None

    def test_uid_with_real_iddeidentifier(self):
        """Test uid passing with real IDDeidentifier transformer."""
        registry = TransformerRegistry(use_defaults=True)

        config = DictConfig(
            {
                "idconfig": {
                    "name": "patient_id",
                    "uid": "patient_id",
                    "description": "Patient identifier",
                }
            }
        )
        transformer = registry.instantiate(
            "IDDeidentifier", config, uid="custom_transformer_uid"
        )

        # IDDeidentifier accepts uid, so it should be set
        assert transformer.uid == "custom_transformer_uid"
        assert transformer.idconfig.name == "patient_id"

    def test_uid_with_real_columndropper(self):
        """Test uid passing with real ColumnDropper transformer."""
        registry = TransformerRegistry(use_defaults=True)

        config = DictConfig(
            {
                "idconfig": {
                    "name": "sensitive_col",
                    "uid": "sensitive_col",
                    "description": "Sensitive column to drop",
                }
            }
        )
        transformer = registry.instantiate(
            "ColumnDropper", config, uid="custom_dropper_uid"
        )

        # ColumnDropper accepts uid, so it should be set
        assert transformer.uid == "custom_dropper_uid"
        assert transformer.idconfig.name == "sensitive_col"
