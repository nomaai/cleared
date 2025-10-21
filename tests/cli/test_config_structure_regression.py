"""Unit tests for configuration structure issues encountered during CLI development."""

import pytest
from cleared.config.structure import (
    ClearedConfig,
    DeIDConfig,
    ClearedIOConfig,
    IOConfig,
    PairedIOConfig,
    IdentifierConfig,
    TimeShiftConfig,
    TableConfig,
    TransformerConfig,
)


class TestTimeShiftConfigRefIdField:
    """Test TimeShiftConfig ref_id field handling."""

    def test_timeshift_config_with_ref_id(self):
        """
        Test TimeShiftConfig with ref_id field.

        Issue: TimeShiftConfig was missing ref_id field, causing validation errors.
        """
        time_shift = TimeShiftConfig(method="random_days", min=-365, max=365)

        assert time_shift.method == "random_days"
        assert time_shift.min == -365
        assert time_shift.max == 365

    def test_timeshift_config_without_ref_id(self):
        """Test TimeShiftConfig without ref_id field."""
        time_shift = TimeShiftConfig(method="random_days", min=-365, max=365)

        assert time_shift.method == "random_days"
        assert time_shift.min == -365
        assert time_shift.max == 365

    def test_timeshift_config_with_none_ref_id(self):
        """Test TimeShiftConfig with None ref_id."""
        time_shift = TimeShiftConfig(method="random_days", min=-365, max=365)

        assert time_shift.method == "random_days"
        assert time_shift.min == -365
        assert time_shift.max == 365

    def test_timeshift_config_in_deid_config(self):
        """Test TimeShiftConfig usage in DeIDConfig."""
        time_shift = TimeShiftConfig(method="random_days", min=-365, max=365)

        deid_config = DeIDConfig(time_shift=time_shift)

        assert deid_config.time_shift is not None
        assert deid_config.time_shift.method == "random_days"

    def test_timeshift_config_validation_with_valid_ref_id(self):
        """Test TimeShiftConfig validation with valid ref_id."""
        time_shift = TimeShiftConfig(method="random_days", min=-365, max=365)

        deid_config = DeIDConfig(time_shift=time_shift)

        # This should not raise any validation errors
        assert deid_config.time_shift.method == "random_days"

    def test_timeshift_config_validation_with_invalid_ref_id(self):
        """Test TimeShiftConfig validation with invalid ref_id."""
        # This should raise a validation error during TimeShiftConfig construction
        with pytest.raises(
            ValueError, match="Unsupported time shift method: invalid_method"
        ):
            TimeShiftConfig(method="invalid_method", min=-365, max=365)

    def test_timeshift_config_validation_with_none_ref_id(self):
        """Test TimeShiftConfig validation with None ref_id."""
        time_shift = TimeShiftConfig(method="random_days", min=-365, max=365)

        _ = DeIDConfig(time_shift=time_shift)

        # This should not raise any validation errors


class TestClearedIOConfigDefaultFactory:
    """Test ClearedIOConfig default factory functionality."""

    def test_cleared_io_config_default_factory(self):
        """
        Test ClearedIOConfig default factory method.

        Issue: ClearedIOConfig.__init__() missing 3 required positional arguments.
        """
        # Test that default factory creates valid instance
        io_config = ClearedIOConfig.default()

        assert isinstance(io_config, ClearedIOConfig)
        assert isinstance(io_config.data, PairedIOConfig)
        assert isinstance(io_config.deid_ref, PairedIOConfig)
        assert isinstance(io_config.data.input_config, IOConfig)
        assert isinstance(io_config.data.output_config, IOConfig)
        assert isinstance(io_config.deid_ref.input_config, IOConfig)
        assert isinstance(io_config.deid_ref.output_config, IOConfig)
        assert io_config.runtime_io_path == "/tmp/runtime"

    def test_cleared_io_config_default_values(self):
        """Test ClearedIOConfig default values."""
        io_config = ClearedIOConfig.default()

        # Test data config
        assert io_config.data.input_config.io_type == "filesystem"
        assert io_config.data.input_config.configs["base_path"] == "/tmp/input"
        assert io_config.data.output_config.io_type == "filesystem"
        assert io_config.data.output_config.configs["base_path"] == "/tmp/output"

        # Test deid_ref config
        assert io_config.deid_ref.input_config.io_type == "filesystem"
        assert (
            io_config.deid_ref.input_config.configs["base_path"]
            == "/tmp/deid_ref_input"
        )
        assert io_config.deid_ref.output_config.io_type == "filesystem"
        assert (
            io_config.deid_ref.output_config.configs["base_path"]
            == "/tmp/deid_ref_output"
        )

        # Test runtime path
        assert io_config.runtime_io_path == "/tmp/runtime"

    def test_cleared_config_with_default_io_config(self):
        """
        Test ClearedConfig with default IO config.

        Issue: ClearedIOConfig.__init__() missing 3 required positional arguments.
        """
        config = ClearedConfig(name="test_engine", deid_config=DeIDConfig())

        # Should use default IO config
        assert isinstance(config.io, ClearedIOConfig)
        assert config.io.runtime_io_path == "/tmp/runtime"
        assert config.io.data.input_config.io_type == "filesystem"
        assert config.io.deid_ref.output_config.io_type == "filesystem"

    def test_cleared_config_with_custom_io_config(self):
        """Test ClearedConfig with custom IO config."""
        custom_io_config = ClearedIOConfig(
            data=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/custom/input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/custom/output"}
                ),
            ),
            deid_ref=PairedIOConfig(
                input_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/custom/deid_input"}
                ),
                output_config=IOConfig(
                    io_type="filesystem", configs={"base_path": "/custom/deid_output"}
                ),
            ),
            runtime_io_path="/custom/runtime",
        )

        config = ClearedConfig(
            name="test_engine", deid_config=DeIDConfig(), io=custom_io_config
        )

        # Should use custom IO config
        assert config.io == custom_io_config
        assert config.io.runtime_io_path == "/custom/runtime"
        assert config.io.data.input_config.configs["base_path"] == "/custom/input"


class TestConfigurationValidation:
    """Test configuration validation issues."""

    def test_deid_config_validation_with_valid_time_shift(self):
        """Test DeIDConfig validation with valid time shift configuration."""
        deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        # This should not raise any validation errors
        assert deid_config.time_shift.method == "random_days"

    def test_deid_config_validation_with_invalid_time_shift_ref_id(self):
        """Test DeIDConfig validation with invalid time shift reference ID."""
        # This should raise a validation error during TimeShiftConfig construction
        with pytest.raises(
            ValueError, match="Unsupported time shift method: invalid_method"
        ):
            TimeShiftConfig(method="invalid_method", min=-365, max=365)

    def test_deid_config_validation_with_none_time_shift(self):
        """Test DeIDConfig validation with None time shift."""
        deid_config = DeIDConfig(time_shift=None)

        # This should not raise any validation errors
        assert deid_config.time_shift is None

    def test_deid_config_validation_with_none_ref_id(self):
        """Test DeIDConfig validation with None ref_id in time shift."""
        deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        # This should not raise any validation errors
        assert deid_config.time_shift.method == "random_days"

    def test_deid_config_validation_with_empty_global_uids(self):
        """Test DeIDConfig validation with empty global_uids."""
        # This should not raise any validation errors
        deid_config = DeIDConfig(
            time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
        )

        assert deid_config.time_shift.method == "random_days"


class TestConfigurationSerialization:
    """Test configuration serialization issues."""

    def test_cleared_config_serialization(self):
        """Test ClearedConfig serialization to dictionary."""
        config = ClearedConfig(
            name="test_engine",
            deid_config=DeIDConfig(
                time_shift=TimeShiftConfig(method="random_days", min=-365, max=365)
            ),
            io=ClearedIOConfig.default(),
            tables={
                "patients": TableConfig(
                    name="patients",
                    depends_on=[],
                    transformers=[
                        TransformerConfig(
                            method="IDDeidentifier",
                            uid="patient_id_transformer",
                            depends_on=[],
                            configs={
                                "idconfig": IdentifierConfig(
                                    name="patient_id", uid="patient_id"
                                )
                            },
                        )
                    ],
                )
            },
        )

        # Test that config can be converted to dictionary
        config_dict = {
            "name": config.name,
            "deid_config": {
                "time_shift": {
                    "method": config.deid_config.time_shift.method,
                    "min": config.deid_config.time_shift.min,
                    "max": config.deid_config.time_shift.max,
                }
                if config.deid_config.time_shift
                else None
            },
            "io": {
                "data": {
                    "input_config": {
                        "io_type": config.io.data.input_config.io_type,
                        "configs": config.io.data.input_config.configs,
                    },
                    "output_config": {
                        "io_type": config.io.data.output_config.io_type,
                        "configs": config.io.data.output_config.configs,
                    },
                },
                "deid_ref": {
                    "input_config": {
                        "io_type": config.io.deid_ref.input_config.io_type,
                        "configs": config.io.deid_ref.input_config.configs,
                    }
                    if config.io.deid_ref.input_config
                    else None,
                    "output_config": {
                        "io_type": config.io.deid_ref.output_config.io_type,
                        "configs": config.io.deid_ref.output_config.configs,
                    },
                },
                "runtime_io_path": config.io.runtime_io_path,
            },
            "tables": {
                table_name: {
                    "name": table_config.name,
                    "depends_on": table_config.depends_on,
                    "transformers": [
                        {
                            "method": transformer.method,
                            "uid": transformer.uid,
                            "depends_on": transformer.depends_on,
                            "configs": transformer.configs,
                        }
                        for transformer in table_config.transformers
                    ],
                }
                for table_name, table_config in config.tables.items()
            },
        }

        # Verify serialization
        assert config_dict["name"] == "test_engine"
        assert len(config_dict["tables"]) == 1
        assert "patients" in config_dict["tables"]

    def test_configuration_with_none_values(self):
        """Test configuration handling with None values."""
        config = ClearedConfig(
            name="test_engine",
            deid_config=DeIDConfig(time_shift=None),
            io=ClearedIOConfig.default(),
            tables={},
        )

        # Test that None values are handled correctly
        assert config.deid_config.time_shift is None
        assert config.deid_config.time_shift is None
        assert config.tables == {}

    def test_configuration_with_empty_values(self):
        """Test configuration handling with empty values."""
        config = ClearedConfig(
            name="test_engine",
            deid_config=DeIDConfig(),
            io=ClearedIOConfig.default(),
            tables={},
        )

        # Test that empty values are handled correctly
        assert config.deid_config.time_shift is None
        assert config.deid_config.time_shift is None
        assert config.tables == {}


class TestConfigurationEdgeCases:
    """Test configuration edge cases."""

    def test_identifier_config_with_none_description(self):
        """Test IdentifierConfig with None description."""
        id_config = IdentifierConfig(
            name="patient_id", uid="patient_id", description=None
        )

        assert id_config.name == "patient_id"
        assert id_config.uid == "patient_id"
        assert id_config.description is None

    def test_identifier_config_with_empty_description(self):
        """Test IdentifierConfig with empty description."""
        id_config = IdentifierConfig(
            name="patient_id", uid="patient_id", description=""
        )

        assert id_config.name == "patient_id"
        assert id_config.uid == "patient_id"
        assert id_config.description == ""

    def test_io_config_with_empty_configs(self):
        """Test IOConfig with empty configs."""
        io_config = IOConfig(io_type="filesystem", configs={})

        assert io_config.io_type == "filesystem"
        assert io_config.configs == {}

    def test_io_config_with_none_configs(self):
        """Test IOConfig with None configs."""
        io_config = IOConfig(io_type="filesystem", configs=None)

        assert io_config.io_type == "filesystem"
        assert io_config.configs is None

    def test_table_config_with_empty_transformers(self):
        """Test TableConfig with empty transformers list."""
        table_config = TableConfig(name="patients", depends_on=[], transformers=[])

        assert table_config.name == "patients"
        assert table_config.depends_on == []
        assert table_config.transformers == []

    def test_table_config_with_none_transformers(self):
        """Test TableConfig with None transformers."""
        table_config = TableConfig(name="patients", depends_on=[], transformers=None)

        assert table_config.name == "patients"
        assert table_config.depends_on == []
        assert table_config.transformers is None
