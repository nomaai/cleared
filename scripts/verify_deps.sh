#!/bin/bash

echo "Verifying dependencies..."

# Function to check fswatch
check_fswatch() {
  if ! command -v fswatch >/dev/null 2>&1; then
    echo "❌ Error: fswatch is not installed!"
    echo ""
    echo "Please install fswatch manually:"
    echo "  macOS: brew install fswatch"
    echo "  Ubuntu: sudo apt install fswatch"
    return 1
  fi
  return 0
}

# Function to check task
check_task() {
  if ! command -v task >/dev/null 2>&1; then
    echo "❌ Error: task (Taskfile) is not installed!"
    echo ""
    echo "Please install task manually:"
    echo "  macOS: brew install go-task/tap/go-task"
    echo "  Ubuntu: sh -c \"\$(curl --location https://taskfile.dev/install.sh)\" -- -d -b /usr/local/bin"
    return 1
  fi
  return 0
}

# Function to check Poetry
check_poetry() {
  if ! command -v poetry >/dev/null 2>&1; then
    echo "❌ Error: Poetry is not installed!"
    echo ""
    echo "Please install Poetry manually:"
    echo "  curl -sSL https://install.python-poetry.org | python3 -"
    return 1
  fi
  return 0
}

# Function to check pre-commit
check_pre_commit() {
  if ! command -v pre-commit >/dev/null 2>&1 && ! poetry run pre-commit --version >/dev/null 2>&1; then
    echo "❌ Error: pre-commit is not installed!"
    echo ""
    echo "Please install pre-commit manually:"
    echo "  macOS: brew install pre-commit"
    echo "  Ubuntu: sudo apt install pre-commit"
    echo "  Or via pip: pip install pre-commit"
    return 1
  fi
  return 0
}

# Function to check actionlint
check_actionlint() {
  if ! command -v actionlint >/dev/null 2>&1; then
    echo "❌ Error: actionlint is not installed!"
    echo ""
    echo "Please install actionlint manually:"
    echo "  macOS: brew install actionlint"
    echo "  Ubuntu: Download from https://github.com/rhysd/actionlint/releases"
    return 1
  fi
  return 0
}

# Function to check yamllint
check_yamllint() {
  if ! command -v yamllint >/dev/null 2>&1; then
    echo "❌ Error: yamllint is not installed!"
    echo ""
    echo "Please install yamllint manually:"
    echo "  macOS: brew install yamllint"
    echo "  Ubuntu: sudo apt install yamllint"
    echo "  Or via pip: pip install yamllint"
    return 1
  fi
  return 0
}

# Check Poetry
if ! check_poetry; then
  exit 1
fi

# Check pre-commit
if ! check_pre_commit; then
  exit 1
fi

# Check actionlint
if ! check_actionlint; then
  exit 1
fi

# Check yamllint
if ! check_yamllint; then
  exit 1
fi

# Check fswatch
if ! check_fswatch; then
  exit 1
fi

# Check task
if ! check_task; then
  exit 1
fi

echo "✅ All required dependencies are installed!"
