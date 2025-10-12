#!/bin/bash

echo "Verifying dependencies..."

# Function to check and install fswatch
check_fswatch() {
  if ! command -v fswatch >/dev/null 2>&1; then
    echo "❌ Error: fswatch is not installed!"
    echo ""
    echo "Do you want me to go ahead and install fswatch? (y/n)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
      echo "Installing fswatch..."
      install_fswatch
      return $?
    else
      echo "No worries! You can install fswatch manually:"
      echo "  macOS: brew install fswatch"
      echo "  Ubuntu: sudo apt install fswatch"
      return 1
    fi
  fi
  return 0
}

# Function to install fswatch based on OS
install_fswatch() {
  OS="$(uname -s)"
  case "$OS" in
    Linux)
      if command -v apt >/dev/null 2>&1; then
        sudo apt update && sudo apt install -y fswatch
        echo "✅ fswatch installed via apt."
      else
        echo "❌ Cannot install fswatch. Please install apt or install manually."
        return 1
      fi
      ;;
    Darwin)
      if command -v brew >/dev/null 2>&1; then
        brew install fswatch
        echo "✅ fswatch installed via Homebrew."
      else
        echo "❌ Cannot install fswatch. Please install Homebrew or install manually."
        return 1
      fi
      ;;
    *)
      echo "❌ Unsupported operating system: $OS"
      return 1
      ;;
  esac
}

# Function to check and install task
check_task() {
  if ! command -v task >/dev/null 2>&1; then
    echo "❌ Error: task (Taskfile) is not installed!"
    echo ""
    echo "Do you want me to go ahead and install task? (y/n)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
      echo "Installing task..."
      install_task
      return $?
    else
      echo "No worries! You can install task manually:"
      echo "  macOS: brew install go-task/tap/go-task"
      echo "  Ubuntu: sh -c \"\$(curl --location https://taskfile.dev/install.sh)\" -- -d -b /usr/local/bin"
      return 1
    fi
  fi
  return 0
}

# Function to install task based on OS
install_task() {
  OS="$(uname -s)"
  case "$OS" in
    Linux)
      if command -v curl >/dev/null 2>&1; then
        sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b /usr/local/bin
        echo "✅ task installed via official installer."
      else
        echo "❌ Cannot install task. Please install curl or install manually."
        return 1
      fi
      ;;
    Darwin)
      if command -v brew >/dev/null 2>&1; then
        brew install go-task/tap/go-task
        echo "✅ task installed via Homebrew."
      else
        echo "❌ Cannot install task. Please install Homebrew or install manually."
        return 1
      fi
      ;;
    *)
      echo "❌ Unsupported operating system: $OS"
      return 1
      ;;
  esac
}

# Function to check and install Poetry
check_poetry() {
  if ! command -v poetry >/dev/null 2>&1; then
    echo "❌ Error: Poetry is not installed!"
    echo ""
    echo "Do you want me to go ahead and install Poetry? (y/n)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
      echo "Installing Poetry..."
      install_poetry
      return $?
    else
      echo "No worries! You can install Poetry manually:"
      echo "  curl -sSL https://install.python-poetry.org | python3 -"
      return 1
    fi
  fi
  return 0
}

# Function to install Poetry
install_poetry() {
  curl -sSL https://install.python-poetry.org | python3 -
  if [ $? -ne 0 ]; then
    echo "❌ Failed to install Poetry."
    return 1
  fi
  echo "✅ Poetry installed successfully."
  
  # Ensure Poetry is in PATH
  OS="$(uname -s)"
  if [[ "$OS" == "Linux" ]]; then
    if ! grep -q 'export PATH="$HOME/.local/bin:$PATH"' ~/.bashrc; then
      echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
      echo "📝 Added Poetry to PATH in ~/.bashrc. Please restart your terminal or run 'source ~/.bashrc'."
    fi
  elif [[ "$OS" == "Darwin" ]]; then
    if ! grep -q 'export PATH="$HOME/.local/bin:$PATH"' ~/.zshrc; then
      echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
      echo "📝 Added Poetry to PATH in ~/.zshrc. Please restart your terminal or run 'source ~/.zshrc'."
    fi
  fi
  
  # Source the relevant rc file to update PATH in current shell
  if [ -f "$HOME/.bashrc" ]; then
    source "$HOME/.bashrc"
  elif [ -f "$HOME/.zshrc" ]; then
    source "$HOME/.zshrc"
  fi
}

# Function to check and install pre-commit
check_pre_commit() {
  if ! command -v pre-commit >/dev/null 2>&1 && ! poetry run pre-commit --version >/dev/null 2>&1; then
    echo "❌ Error: pre-commit is not installed!"
    echo ""
    echo "Do you want me to go ahead and install pre-commit? (y/n)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
      echo "Installing pre-commit..."
      install_pre_commit
      return $?
    else
      echo "No worries! You can install pre-commit manually:"
      echo "  macOS: brew install pre-commit"
      echo "  Ubuntu: sudo apt install pre-commit"
      echo "  Or via pip: pip install pre-commit"
      return 1
    fi
  fi
  return 0
}

# Function to install pre-commit based on OS
install_pre_commit() {
  OS="$(uname -s)"
  case "$OS" in
    Linux)
      if command -v apt >/dev/null 2>&1; then
        sudo apt update && sudo apt install -y pre-commit
        echo "✅ pre-commit installed via apt."
      elif command -v pip3 >/dev/null 2>&1; then
        pip3 install pre-commit
        echo "✅ pre-commit installed via pip3."
      else
        echo "❌ Cannot install pre-commit. Please install apt or pip3."
        return 1
      fi
      ;;
    Darwin)
      if command -v brew >/dev/null 2>&1; then
        brew install pre-commit
        echo "✅ pre-commit installed via Homebrew."
      elif command -v pip3 >/dev/null 2>&1; then
        pip3 install pre-commit
        echo "✅ pre-commit installed via pip3."
      else
        echo "❌ Cannot install pre-commit. Please install Homebrew or pip3."
        return 1
      fi
      ;;
    *)
      echo "❌ Unsupported operating system: $OS"
      return 1
      ;;
  esac
}

# Check Poetry
if ! check_poetry; then
  exit 1
fi

# Check pre-commit
if ! check_pre_commit; then
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
