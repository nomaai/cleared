"""The root package of the project."""

# Version is read from VERSION file at package root
from pathlib import Path

# Import all main components for easy access
from .transformers import (
    IDDeidentifier,
    DateTimeDeidentifier,
    ColumnDropper,
    FilterableTransformer,
    TablePipeline,
    TransformerRegistry,
)
from .config import (
    IdentifierConfig,
    TimeShiftConfig,
    DeIDConfig,
    FilterConfig,
    IOConfig,
    PairedIOConfig,
    ClearedIOConfig,
    ClearedConfig,
)
from .engine import ClearedEngine
from .sample import sample_data
from .logging_config import setup_logging, get_logger


# Get version from VERSION file
_VERSION_FILE = Path(__file__).parent.parent / "VERSION"
if _VERSION_FILE.exists():
    __version__ = _VERSION_FILE.read_text().strip()
else:
    # Fallback: try to read from pyproject.toml
    try:
        _PYPROJECT = Path(__file__).parent.parent / "pyproject.toml"
        if _PYPROJECT.exists():
            # Try tomllib first (Python 3.11+), fallback to tomli (Python 3.10)
            try:
                import tomllib

                with open(_PYPROJECT, "rb") as f:
                    _data = tomllib.load(f)
            except ImportError:
                # Python 3.10 fallback
                import tomli as tomllib

                with open(_PYPROJECT, "rb") as f:
                    _data = tomllib.load(f)
            __version__ = _data["tool"]["poetry"]["version"]
        else:
            __version__ = "0.0.0"
    except (ImportError, KeyError):
        __version__ = "0.0.0"


__all__ = [
    "ClearedConfig",
    "ClearedEngine",
    "ClearedIOConfig",
    "ColumnDropper",
    "DateTimeDeidentifier",
    "DeIDConfig",
    "FilterConfig",
    "FilterableTransformer",
    "IDDeidentifier",
    "IOConfig",
    "IdentifierConfig",
    "PairedIOConfig",
    "TablePipeline",
    "TimeShiftConfig",
    "TransformerRegistry",
    "__version__",
    "get_logger",
    "sample_data",
    "setup_logging",
]
