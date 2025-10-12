#!/bin/bash

echo "🧹 Cleaning unused files..."

# Clean Python cache files
find . -name '*.pyc' -delete 2>/dev/null || true
find . -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true

# Clean system files
find . -name 'Thumbs.db' -delete 2>/dev/null || true
find . -name '*~' -delete 2>/dev/null || true

# Clean build and cache directories
rm -rf .cache 2>/dev/null || true
rm -rf .pytest_cache 2>/dev/null || true
rm -rf .mypy_cache 2>/dev/null || true
rm -rf dist 2>/dev/null || true
rm -rf .tox/ 2>/dev/null || true
rm -rf docs/_build 2>/dev/null || true
rm -rf .ruff_cache 2>/dev/null || true

# Clean test and coverage files
rm -rf pytest-coverage.txt 2>/dev/null || true
rm -rf pytest.xml 2>/dev/null || true
rm -rf htmlcov/ 2>/dev/null || true
rm -rf coverage.xml 2>/dev/null || true

# Clean Poetry cache
poetry cache clear --all . --no-interaction 2>/dev/null || true

echo "✅ Cleanup completed!"
