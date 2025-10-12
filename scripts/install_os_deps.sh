#!/bin/bash

set -e

echo "🔧 Installing OS dependencies for cleared project..."

# Detect OS
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS="ubuntu"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macos"
else
    echo "❌ Error: Unsupported operating system: $OSTYPE"
    echo "This script supports Ubuntu and macOS only."
    exit 1
fi

echo "📋 Detected OS: $OS"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Install Poetry
echo "📦 Installing Poetry..."
if command_exists poetry; then
    echo "✅ Poetry is already installed"
    poetry --version
else
    echo "📥 Downloading and installing Poetry..."
    curl -sSL https://install.python-poetry.org | python3 -
    
    # Add Poetry to PATH for current session
    export PATH="$HOME/.local/bin:$PATH"
    
    # Add Poetry to PATH permanently
    if [[ "$OS" == "ubuntu" ]]; then
        echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
        echo "📝 Added Poetry to PATH in ~/.bashrc"
    elif [[ "$OS" == "macos" ]]; then
        echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
        echo "📝 Added Poetry to PATH in ~/.zshrc"
    fi
    
    echo "✅ Poetry installed successfully"
    poetry --version
fi

# Install pre-commit
echo "🔧 Installing pre-commit..."
if command_exists pre-commit; then
    echo "✅ pre-commit is already installed"
    pre-commit --version
else
    echo "📥 Installing pre-commit..."
    if [[ "$OS" == "ubuntu" ]]; then
        # For Ubuntu, try pip first, then apt
        if command_exists pip3; then
            pip3 install pre-commit
        elif command_exists pip; then
            pip install pre-commit
        else
            echo "📦 Installing pip first..."
            sudo apt update
            sudo apt install -y python3-pip
            pip3 install pre-commit
        fi
    elif [[ "$OS" == "macos" ]]; then
        # For macOS, try pip first, then brew
        if command_exists pip3; then
            pip3 install pre-commit
        elif command_exists pip; then
            pip install pre-commit
        elif command_exists brew; then
            brew install pre-commit
        else
            echo "📦 Installing pip first..."
            curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
            python3 get-pip.py
            rm get-pip.py
            pip3 install pre-commit
        fi
    fi
    
    echo "✅ pre-commit installed successfully"
    pre-commit --version
fi

echo ""
echo "🎉 All OS dependencies installed successfully!"
echo ""
echo "📋 Next steps:"
echo "   1. Restart your terminal or run: source ~/.bashrc (Ubuntu) or source ~/.zshrc (macOS)"
echo "   2. Run: task setup-env"
echo ""
