#!/bin/bash

# Check if silent mode is requested
SILENT=false
if [ "$1" = "silent" ]; then
  SILENT=true
fi

if [ "$SILENT" = false ]; then
  echo "🔍 Verifying environment setup..."
fi

# Check if Python virtual environment exists
if [ ! -d ".venv" ] && [ ! -d "venv" ] && ! poetry env info >/dev/null 2>&1; then
  if [ "$SILENT" = false ]; then
    echo "❌ No Python virtual environment found!"
    echo "   Run 'task setup-env' to create the environment."
  fi
  exit 1
fi

# Check if cleared package is installed
if ! poetry run python -c "import cleared" >/dev/null 2>&1; then
  if [ "$SILENT" = false ]; then
    echo "❌ Cleared package is not installed in the environment!"
    echo "   Run 'task setup-env' to install dependencies."
  fi
  exit 1
fi

# Check if pre-commit is available
if ! poetry run pre-commit --version >/dev/null 2>&1; then
  if [ "$SILENT" = false ]; then
    echo "❌ pre-commit is not available in the environment!"
    echo "   Run 'task setup-env' to install pre-commit hooks."
  fi
  exit 1
fi

# Check for pre-commit configuration file
if [ ! -f ".pre-commit-config.yaml" ]; then
  if [ "$SILENT" = false ]; then
    echo "⚠️  Warning: .pre-commit-config.yaml not found!"
    echo "   Pre-commit hooks may not be properly configured."
  fi
fi

# Check if Git pre-commit hooks are installed
if [ ! -d ".git/hooks" ] || [ ! -f ".git/hooks/pre-commit" ]; then
  if [ "$SILENT" = false ]; then
    echo "⚠️  Warning: Git pre-commit hooks not installed!"
    echo "   Run 'task setup-env' to install pre-commit hooks."
  fi
fi

if [ "$SILENT" = false ]; then
  echo "✅ Environment verification completed!"
  echo "   Python environment: $(poetry env info --path 2>/dev/null || echo 'Not found')"
  echo "   Cleared package: $(poetry run python -c 'import cleared; print(cleared.__version__ if hasattr(cleared, "__version__") else "installed")' 2>/dev/null || echo 'Not available')"
  echo "   Pre-commit: $(poetry run pre-commit --version 2>/dev/null | head -1 || echo 'Not available')"
fi
