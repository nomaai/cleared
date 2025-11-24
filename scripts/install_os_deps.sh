#!/bin/bash
set -e

echo "🔧 Installing OS dependencies for cleared project..."

OS="$(uname -s)"

# Function to install Poetry
install_poetry() {
    echo "📦 Installing Poetry..."
    if command -v poetry >/dev/null 2>&1; then
        echo "✅ Poetry is already installed"
    else
        curl -sSL https://install.python-poetry.org | python3 -
        if [ $? -ne 0 ]; then
            echo "❌ Failed to install Poetry."
            exit 1
        fi
        echo "✅ Poetry installed successfully."
    fi
    # Ensure Poetry is in PATH
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

# Function to install pre-commit
install_pre_commit() {
    echo "🔧 Installing pre-commit..."
    if command -v pre-commit >/dev/null 2>&1; then
        echo "✅ pre-commit is already installed"
    elif command -v pip3 >/dev/null 2>&1; then
        pip3 install pre-commit
        echo "✅ pre-commit installed via pip3."
    elif command -v pip >/dev/null 2>&1; then
        pip install pre-commit
        echo "✅ pre-commit installed via pip."
    elif [[ "$OS" == "Linux" ]]; then
        if command -v apt >/dev/null 2>&1; then
            sudo apt update && sudo apt install -y pre-commit
            echo "✅ pre-commit installed via apt."
        else
            echo "❌ Cannot install pre-commit. Please install pip or apt."
            exit 1
        fi
    elif [[ "$OS" == "Darwin" ]]; then
        if command -v brew >/dev/null 2>&1; then
            brew install pre-commit
            echo "✅ pre-commit installed via Homebrew."
        else
            echo "❌ Cannot install pre-commit. Please install Homebrew or pip."
            exit 1
        fi
    else
        echo "❌ Cannot install pre-commit. Please install pip or a package manager."
        exit 1
    fi
}

# Function to install actionlint with retry
install_actionlint() {
    echo "🔧 Installing actionlint..."
    if command -v actionlint >/dev/null 2>&1; then
        echo "✅ actionlint is already installed"
    else
        if [[ "$OS" == "Linux" ]]; then
            # Download and install actionlint for Linux with retry logic
            MAX_RETRIES=3
            RETRY_DELAY=60
            RETRY_COUNT=0
            
            while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
                RETRY_COUNT=$((RETRY_COUNT + 1))
                echo "📥 Attempting to install actionlint (attempt $RETRY_COUNT/$MAX_RETRIES)..."
                
                LATEST_URL=$(curl -s https://api.github.com/repos/rhysd/actionlint/releases/latest | grep "browser_download_url.*linux_amd64.tar.gz" | cut -d '"' -f 4)
                
                if [ -z "$LATEST_URL" ]; then
                    echo "⚠️  Failed to get actionlint download URL."
                    if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                        echo "⏳ Waiting ${RETRY_DELAY} seconds before retry..."
                        sleep $RETRY_DELAY
                        continue
                    else
                        echo "❌ Failed to get actionlint download URL after $MAX_RETRIES attempts."
                        return 1
                    fi
                fi
                
                echo "📥 Downloading actionlint from: $LATEST_URL"
                TEMP_FILE=$(mktemp)
                
                if curl -sL "$LATEST_URL" -o "$TEMP_FILE"; then
                    echo "📦 Extracting actionlint..."
                    if tar -xzf "$TEMP_FILE" 2>/dev/null; then
                        rm -f "$TEMP_FILE"
                        sudo mv actionlint /usr/local/bin/ 2>/dev/null || mv actionlint /usr/local/bin/
                        echo "✅ actionlint installed via direct download."
                        return 0
                    else
                        echo "⚠️  Failed to extract actionlint archive."
                        rm -f "$TEMP_FILE"
                        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                            echo "⏳ Waiting ${RETRY_DELAY} seconds before retry..."
                            sleep $RETRY_DELAY
                            continue
                        else
                            echo "❌ Failed to extract actionlint archive after $MAX_RETRIES attempts."
                            return 1
                        fi
                    fi
                else
                    echo "⚠️  Failed to download actionlint from $LATEST_URL"
                    rm -f "$TEMP_FILE"
                    if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                        echo "⏳ Waiting ${RETRY_DELAY} seconds before retry..."
                        sleep $RETRY_DELAY
                        continue
                    else
                        echo "❌ Failed to download actionlint after $MAX_RETRIES attempts."
                        return 1
                    fi
                fi
            done
        elif [[ "$OS" == "Darwin" ]]; then
            if command -v brew >/dev/null 2>&1; then
                brew install actionlint
                echo "✅ actionlint installed via Homebrew."
            else
                # Download and install actionlint for macOS with retry logic
                MAX_RETRIES=3
                RETRY_DELAY=2
                RETRY_COUNT=0
                
                while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
                    RETRY_COUNT=$((RETRY_COUNT + 1))
                    echo "📥 Attempting to install actionlint (attempt $RETRY_COUNT/$MAX_RETRIES)..."
                    
                    LATEST_URL=$(curl -s https://api.github.com/repos/rhysd/actionlint/releases/latest | grep "browser_download_url.*darwin_amd64.tar.gz" | cut -d '"' -f 4)
                    
                    if [ -z "$LATEST_URL" ]; then
                        echo "⚠️  Failed to get actionlint download URL."
                        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                            echo "⏳ Waiting ${RETRY_DELAY} seconds before retry..."
                            sleep $RETRY_DELAY
                            continue
                        else
                            echo "❌ Failed to get actionlint download URL after $MAX_RETRIES attempts."
                            return 1
                        fi
                    fi
                    
                    echo "📥 Downloading actionlint from: $LATEST_URL"
                    TEMP_FILE=$(mktemp)
                    
                    if curl -sL "$LATEST_URL" -o "$TEMP_FILE"; then
                        echo "📦 Extracting actionlint..."
                        if tar -xzf "$TEMP_FILE" 2>/dev/null; then
                            rm -f "$TEMP_FILE"
                            sudo mv actionlint /usr/local/bin/ 2>/dev/null || mv actionlint /usr/local/bin/
                            echo "✅ actionlint installed via direct download."
                            return 0
                        else
                            echo "⚠️  Failed to extract actionlint archive."
                            rm -f "$TEMP_FILE"
                            if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                                echo "⏳ Waiting ${RETRY_DELAY} seconds before retry..."
                                sleep $RETRY_DELAY
                                continue
                            else
                                echo "❌ Failed to extract actionlint archive after $MAX_RETRIES attempts."
                                return 1
                            fi
                        fi
                    else
                        echo "⚠️  Failed to download actionlint from $LATEST_URL"
                        rm -f "$TEMP_FILE"
                        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                            echo "⏳ Waiting ${RETRY_DELAY} seconds before retry..."
                            sleep $RETRY_DELAY
                            continue
                        else
                            echo "❌ Failed to download actionlint after $MAX_RETRIES attempts."
                            return 1
                        fi
                    fi
                done
            fi
        else
            echo "❌ Cannot install actionlint. Please install manually from https://github.com/rhysd/actionlint"
            exit 1
        fi
    fi
}

# Function to install yamllint
install_yamllint() {
    echo "🔧 Installing yamllint..."
    if command -v yamllint >/dev/null 2>&1; then
        echo "✅ yamllint is already installed"
    elif command -v pip3 >/dev/null 2>&1; then
        pip3 install yamllint
        echo "✅ yamllint installed via pip3."
    elif command -v pip >/dev/null 2>&1; then
        pip install yamllint
        echo "✅ yamllint installed via pip."
    elif [[ "$OS" == "Linux" ]]; then
        if command -v apt >/dev/null 2>&1; then
            sudo apt update && sudo apt install -y yamllint
            echo "✅ yamllint installed via apt."
        else
            echo "❌ Cannot install yamllint. Please install pip or apt."
            exit 1
        fi
    elif [[ "$OS" == "Darwin" ]]; then
        if command -v brew >/dev/null 2>&1; then
            brew install yamllint
            echo "✅ yamllint installed via Homebrew."
        else
            echo "❌ Cannot install yamllint. Please install Homebrew."
            exit 1
        fi
    else
        echo "❌ Cannot install yamllint. Please install pip or a package manager."
        exit 1
    fi
}

# Function to install fswatch
install_fswatch() {
    echo "🔧 Installing fswatch..."
    if command -v fswatch >/dev/null 2>&1; then
        echo "✅ fswatch is already installed"
    elif [[ "$OS" == "Linux" ]]; then
        if command -v apt >/dev/null 2>&1; then
            sudo apt update && sudo apt install -y fswatch
            echo "✅ fswatch installed via apt."
        else
            echo "❌ Cannot install fswatch. Please install apt or install manually."
            exit 1
        fi
    elif [[ "$OS" == "Darwin" ]]; then
        if command -v brew >/dev/null 2>&1; then
            brew install fswatch
            echo "✅ fswatch installed via Homebrew."
        else
            echo "❌ Cannot install fswatch. Please install Homebrew or install manually."
            exit 1
        fi
    else
        echo "❌ Cannot install fswatch. Please install manually."
        exit 1
    fi
}

echo "📋 Detected OS: $(echo "$OS" | tr '[:upper:]' '[:lower:]')"

case "$OS" in
    Linux)
        install_poetry
        install_pre_commit
        install_actionlint
        install_yamllint
        install_fswatch
        ;;
    Darwin)
        install_poetry
        install_pre_commit
        install_actionlint
        install_yamllint
        install_fswatch
        ;;
    *)
        echo "❌ Unsupported operating system: $OS"
        exit 1
        ;;
esac

echo ""
echo "🎉 All OS dependencies installed successfully!"
echo ""
echo "📋 Next steps:"
echo "   1. Restart your terminal or run: source ~/.bashrc (Ubuntu) or source ~/.zshrc (macOS)"
echo "   2. Run: task setup-env"
echo ""