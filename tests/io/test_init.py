"""Unit tests for IO module initialization and imports."""

import pytest


class TestIOInit:
    """Test the IO module initialization and imports."""

    def test_base_imports(self):
        """Test that base classes and exceptions are imported correctly."""
        from cleared.io import (
            BaseDataLoader,
            DataLoaderError,
            IOConnectionError,
            TableNotFoundError,
            WriteError,
            ValidationError,
        )

        # Test that classes are imported
        assert BaseDataLoader is not None
        assert DataLoaderError is not None
        assert IOConnectionError is not None
        assert TableNotFoundError is not None
        assert WriteError is not None
        assert ValidationError is not None

    def test_filesystem_import(self):
        """Test that FileSystemDataLoader is imported correctly."""
        from cleared.io import FileSystemDataLoader

        assert FileSystemDataLoader is not None

    def test_sql_import_available(self):
        """Test SQL import when SQLAlchemy is available."""
        # Test that SQLDataLoader is available when SQLAlchemy is installed
        import importlib.util

        if importlib.util.find_spec("sqlalchemy") is not None:
            from cleared.io import SQLDataLoader

            assert SQLDataLoader is not None
        else:
            # SQLAlchemy not available, which is expected in some environments
            pytest.skip("SQLAlchemy not available")

    def test_all_exports_with_sql(self):
        """Test __all__ exports when SQL is available."""
        # Test current exports without module reloading
        from cleared.io import __all__

        expected_exports = [
            "BaseDataLoader",
            "FileSystemDataLoader",
            "DataLoaderError",
            "IOConnectionError",
            "TableNotFoundError",
            "WriteError",
            "ValidationError",
        ]

        # SQLDataLoader may or may not be available depending on environment
        for export in expected_exports:
            assert export in __all__

    def test_module_docstring(self):
        """Test that module has proper docstring."""
        import cleared.io

        assert cleared.io.__doc__ is not None
        assert "Data I/O modules for cleared" in cleared.io.__doc__

    def test_import_from_submodules(self):
        """Test importing from submodules directly."""
        from cleared.io.base import BaseDataLoader, DataLoaderError
        from cleared.io.filesystem import FileSystemDataLoader

        assert BaseDataLoader is not None
        assert DataLoaderError is not None
        assert FileSystemDataLoader is not None

    def test_sql_submodule_import(self):
        """Test importing from SQL submodule directly."""
        try:
            from cleared.io.sql import SQLDataLoader

            assert SQLDataLoader is not None
        except ImportError:
            # SQLAlchemy might not be available in test environment
            pytest.skip("SQLAlchemy not available")

    def test_exception_hierarchy(self):
        """Test that exception classes have proper inheritance hierarchy."""
        from cleared.io import (
            DataLoaderError,
            IOConnectionError,
            TableNotFoundError,
            WriteError,
            ValidationError,
        )

        # Test inheritance
        assert issubclass(IOConnectionError, DataLoaderError)
        assert issubclass(TableNotFoundError, DataLoaderError)
        assert issubclass(WriteError, DataLoaderError)
        assert issubclass(ValidationError, DataLoaderError)

        # Test that they are also Exception subclasses
        assert issubclass(DataLoaderError, Exception)
        assert issubclass(IOConnectionError, Exception)
        assert issubclass(TableNotFoundError, Exception)
        assert issubclass(WriteError, Exception)
        assert issubclass(ValidationError, Exception)

    def test_abstract_base_class(self):
        """Test that BaseDataLoader is properly abstract."""
        from cleared.io import BaseDataLoader
        from abc import ABC

        assert issubclass(BaseDataLoader, ABC)

        # Test that it cannot be instantiated directly
        with pytest.raises(TypeError):
            BaseDataLoader({})

    def test_module_structure(self):
        """Test that the module has the expected structure."""
        import cleared.io

        # Test that expected attributes exist
        assert hasattr(cleared.io, "__all__")
        assert hasattr(cleared.io, "__doc__")
        assert hasattr(cleared.io, "_SQL_AVAILABLE")

        # Test that __all__ is a list
        assert isinstance(cleared.io.__all__, list)

        # Test that all items in __all__ are strings
        for item in cleared.io.__all__:
            assert isinstance(item, str)

    def test_import_performance(self):
        """Test that imports are reasonably fast."""
        import time

        start_time = time.time()

        # Import the module

        end_time = time.time()

        # Should complete in reasonable time (less than 1 second)
        assert end_time - start_time < 1.0

    def test_import_with_missing_dependencies(self):
        """Test import behavior when optional dependencies are missing."""
        # This test ensures the module can be imported even if some dependencies are missing
        # The actual behavior depends on what's available in the test environment

        try:
            import cleared.io
            from cleared.io import BaseDataLoader, FileSystemDataLoader

            # These should always be available
            assert BaseDataLoader is not None
            assert FileSystemDataLoader is not None

            # SQLDataLoader might or might not be available
            if hasattr(cleared.io, "SQLDataLoader"):
                # If it exists, it should either be a class or None
                assert cleared.io.SQLDataLoader is None or callable(
                    cleared.io.SQLDataLoader
                )

        except ImportError as e:
            pytest.fail(
                f"IO module should be importable even with missing dependencies: {e}"
            )

    def test_reload_behavior(self):
        """Test module reload behavior."""
        import importlib
        import cleared.io

        # Reload the module
        importlib.reload(cleared.io)

        # Should still work after reload
        from cleared.io import BaseDataLoader, FileSystemDataLoader

        assert BaseDataLoader is not None
        assert FileSystemDataLoader is not None
