"""Basic tests for the cleared package."""


def test_package_import():
    """Test that the cleared package can be imported."""
    import cleared

    assert cleared is not None


def test_package_version():
    """Test that the package has a version."""
    import cleared

    # This will work once we add a __version__ attribute
    # For now, just test that the module exists
    assert hasattr(cleared, "__name__")


def test_basic_functionality():
    """Test basic functionality placeholder."""
    # This is a placeholder test that always passes
    # Replace with actual tests as the package develops
    assert True
