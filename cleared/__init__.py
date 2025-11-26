"""The root package of the project."""

# Version is read from VERSION file at package root
from pathlib import Path

# Get version from VERSION file
_VERSION_FILE = Path(__file__).parent.parent.parent / "VERSION"
if _VERSION_FILE.exists():
    __version__ = _VERSION_FILE.read_text().strip()
else:
    # Fallback: try to read from pyproject.toml
    try:
        import tomllib

        _PYPROJECT = Path(__file__).parent.parent.parent / "pyproject.toml"
        if _PYPROJECT.exists():
            with open(_PYPROJECT, "rb") as f:
                _data = tomllib.load(f)
                __version__ = _data["tool"]["poetry"]["version"]
        else:
            __version__ = "0.0.0"
    except (ImportError, KeyError):
        __version__ = "0.0.0"

# Import all main components for easy access
from .transformers import (  # noqa: E402
    IDDeidentifier,
    DateTimeDeidentifier,
    ColumnDropper,
    FilterableTransformer,
    TablePipeline,
    TransformerRegistry,
)
from .config import (  # noqa: E402
    IdentifierConfig,
    TimeShiftConfig,
    DeIDConfig,
    FilterConfig,
    IOConfig,
    PairedIOConfig,
    ClearedIOConfig,
    ClearedConfig,
)
from .engine import ClearedEngine  # noqa: E402
from .sample import sample_data  # noqa: E402
from .logging_config import setup_logging, get_logger  # noqa: E402

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
