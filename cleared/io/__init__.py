"""Data I/O modules for cleared."""

from omegaconf import DictConfig
from .base import (
    BaseDataLoader,
    DataLoaderError,
    IOConnectionError,
    TableNotFoundError,
    WriteError,
    ValidationError,
    FileFormatError,
)
from .filesystem import FileSystemDataLoader

try:
    from .sql import SQLDataLoader

    _SQL_AVAILABLE = True
except ImportError:
    _SQL_AVAILABLE = False
    SQLDataLoader = None

__all__ = [
    "BaseDataLoader",
    "DataLoaderError",
    "FileFormatError",
    "FileSystemDataLoader",
    "IOConnectionError",
    "TableNotFoundError",
    "ValidationError",
    "WriteError",
]

if _SQL_AVAILABLE:
    __all__.append("SQLDataLoader")


def create_data_loader(io_config: DictConfig) -> BaseDataLoader:
    """
    Create a data loader based on the IO configuration.

    Args:
        io_config: IO configuration as a DictConfig

    Returns:
        Data loader

    Raises:
        ValueError: If unsupported IO type is specified

    """
    # Create config dict with data_source_type and connection_params
    config_dict = {
        "data_source_type": io_config.io_type,
        "connection_params": io_config.configs,
    }

    if io_config.io_type == "filesystem":
        return FileSystemDataLoader(config_dict)
    elif io_config.io_type == "sql":
        return SQLDataLoader(config_dict)
    else:
        raise ValueError(f"Unsupported IO type: {io_config.io_type}")
