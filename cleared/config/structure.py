"""
Configuration structure for cleared.

This module defines the dataclasses used for configuration throughout the Cleared framework.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class IdentifierConfig:
    """Configuration for identifiers."""

    name: str
    uid: str
    description: str | None = None

    def deid_uid(self) -> str:
        """Get the de-identification UID for the identifier."""
        return f"{self.uid}__deid"


@dataclass
class TimeShiftConfig:
    """Configuration for time shift operations."""

    method: str
    min: int | None = None
    max: int | None = None

    def __post_init__(self):
        """Validate that method is a supported time shift method."""
        valid_methods = [
            "shift_by_hours",
            "shift_by_days",
            "shift_by_weeks",
            "shift_by_months",
            "shift_by_years",
            "random_days",
            "random_hours",
        ]
        if self.method not in valid_methods:
            raise ValueError(f"Unsupported time shift method: {self.method}")


@dataclass
class DeIDConfig:
    """Configuration for de-identification operations."""

    time_shift: TimeShiftConfig | None = None


@dataclass
class TransformerConfig:
    """Configuration for a transformer."""

    method: str
    uid: str | None = None
    depends_on: list[str] = field(default_factory=list)
    configs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate that method is a valid transformer name."""
        # This import is here to avoid circular dependencies
        from cleared.transformers.registry import get_expected_transformer_names

        if self.method not in get_expected_transformer_names():
            raise ValueError(
                f"method must be a valid transformer name. "
                f"method: {self.method}, valid transformer names: {get_expected_transformer_names()}"
            )


@dataclass
class TableConfig:
    """Configuration for a table in the pipeline."""

    name: str
    depends_on: list[str] = field(default_factory=list)
    transformers: list[TransformerConfig] = field(default_factory=list)


@dataclass
class IOConfig:
    """Configuration for data input/output operations."""

    io_type: str  # "filesystem" or "sql"
    suffix: str | None = None
    configs: dict[str, Any] = field(default_factory=dict)


@dataclass
class PairedIOConfig:
    """Configuration for paired input/output operations."""

    input_config: IOConfig
    output_config: IOConfig


@dataclass
class ClearedIOConfig:
    """Configuration for Cleared I/O operations."""

    data: PairedIOConfig
    deid_ref: PairedIOConfig
    runtime_io_path: str

    @classmethod
    def default(cls) -> "ClearedIOConfig":
        """Create a default ClearedIOConfig instance."""
        default_input = IOConfig(
            io_type="filesystem", configs={"base_path": "/tmp/input"}
        )
        default_output = IOConfig(
            io_type="filesystem", configs={"base_path": "/tmp/output"}
        )
        default_data = PairedIOConfig(
            input_config=default_input, output_config=default_output
        )

        default_deid_input = IOConfig(
            io_type="filesystem", configs={"base_path": "/tmp/deid_ref_input"}
        )
        default_deid_output = IOConfig(
            io_type="filesystem", configs={"base_path": "/tmp/deid_ref_output"}
        )
        default_deid_ref = PairedIOConfig(
            input_config=default_deid_input, output_config=default_deid_output
        )

        return cls(
            data=default_data, deid_ref=default_deid_ref, runtime_io_path="/tmp/runtime"
        )


@dataclass
class ClearedConfig:
    """Main configuration class for Cleared."""

    name: str
    deid_config: DeIDConfig = field(default_factory=DeIDConfig)
    io: ClearedIOConfig = field(default_factory=ClearedIOConfig.default)
    tables: dict[str, TableConfig] = field(default_factory=dict)
