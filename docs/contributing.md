# Contributing to Cleared

Thank you for your interest in contributing to Cleared! This guide will help you get started with the development workflow.

## Prerequisites

Before you begin, ensure you have:

- **Python 3.10+** installed
- **Git** installed and configured
- **Terminal/command line** access
- **GitHub account** (for submitting pull requests)

## Quick Start

### 1. Fork and Clone the Repository

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/cleared.git
cd cleared
```

### 2. Install Dependencies

The project uses Poetry for dependency management and Task runner for automation. Run these commands in order:

```bash
# Install OS dependencies (Poetry, pre-commit, actionlint, yamllint)
task install-os-deps

# Setup Python development environment
task setup-env

# Verify everything is working
task verify-env
```

### 3. Verify Installation

```bash
# Check environment and run tests
task show
task test
```

## Development Workflow

### Daily Development

For active development, use the watch mode to automatically run quality checks:

```bash
# Auto-run lint, format, and test on file changes
task watch
```

This will monitor Python files and automatically run:
- **Linting** (code quality checks)
- **Formatting** (code style)
- **Testing** (unit tests)

### Manual Quality Checks

You can also run quality checks manually:

```bash
# Run linter
task lint

# Auto-fix linting issues
task lint-fix

# Format code
task format

# Run tests
task test
```

### Code Quality Standards

The project enforces code quality through:

- **Pre-commit hooks** - Automatically run on every commit
- **CI/CD pipeline** - Runs on every pull request
- **Ruff** - Fast Python linter and formatter
- **Pytest** - Testing framework with coverage reporting

## Making Changes

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-description
```

### 2. Make Your Changes

- Write your code following the project's style guidelines
- Add tests for new functionality
- Update documentation if needed
- Ensure all tests pass: `task test`

### 3. Commit Your Changes

```bash
# Stage your changes
git add .

# Commit with a descriptive message
git commit -m "Add feature: brief description of changes"
```

**Note**: Pre-commit hooks will automatically run linting and formatting before the commit is accepted.

### 4. Push and Create Pull Request

```bash
# Push your branch
git push origin feature/your-feature-name

# Create a pull request on GitHub
```

## Available Commands

The project uses Task runner for automation. Here are the key commands:

### Environment Setup
- `task install-os-deps` - Install system dependencies (first time only)
- `task setup-env` - Setup Python development environment
- `task verify-env` - Verify environment is ready
- `task show` - Display environment information

### Development
- `task watch` - Auto-run quality checks on file changes
- `task lint` - Run code linter
- `task lint-fix` - Auto-fix linting issues
- `task format` - Format code
- `task test` - Run tests with coverage

### Documentation
- `task docs` - Build and open documentation
- `task help` - Show all available commands

### Maintenance
- `task clean` - Clean build artifacts and cache files

## Testing

### Running Tests

```bash
# Run all tests
task test

# Run tests with verbose output
poetry run pytest -v

# Run specific test file
poetry run pytest tests/test_specific.py
```

### Test Coverage

The project uses pytest with coverage reporting. Coverage reports are generated in:
- Terminal output
- `htmlcov/index.html` (HTML report)
- `coverage.xml` (XML report)

## Code Style

The project uses **Ruff** for linting and formatting, which enforces:

- **PEP 8** style guidelines
- **Import sorting** (isort)
- **Code formatting** (black-like)
- **Bug detection** (flake8-bugbear)
- **Security checks**

### Pre-commit Hooks

Pre-commit hooks automatically run on every commit:
- **actionlint** - GitHub Actions workflow validation
- **yamllint** - YAML file validation
- **ruff** - Python linting and formatting
- **pytest** - Test execution (on pre-push)

## Pull Request Process

### Before Submitting

1. **Run quality checks**: `task lint && task format && task test`
2. **Update documentation** if needed
3. **Add tests** for new functionality
4. **Self-review** your code

### Pull Request Template

When creating a pull request, please:

1. **Describe your changes** clearly
2. **Link to related issues** using "Fixes #123"
3. **Explain your testing approach**
4. **Check the required boxes** in the PR template

### Review Process

- All PRs require review before merging
- CI must pass (linting, formatting, testing)
- Code must follow project style guidelines
- Tests must pass with good coverage

## Troubleshooting

### Common Issues

**Environment Issues:**
```bash
# Reinstall dependencies
task clean
task setup-env
```

**Pre-commit Hook Issues:**
```bash
# Reinstall pre-commit hooks
poetry run pre-commit install --hook-type pre-commit --hook-type pre-push
```

**Dependency Issues:**
```bash
# Check and install missing dependencies
task verify-deps
```

### Getting Help

- **Check existing issues** on GitHub
- **Read the documentation** in the `docs/` folder
- **Run `task help`** for available commands
- **Create a new issue** if you can't find a solution

## Project Structure

```
cleared/
├── cleared/           # Main package code
├── tests/            # Test files
├── docs/             # Documentation
├── scripts/          # Utility scripts
├── .github/          # GitHub workflows and templates
├── Taskfile.yml      # Task runner configuration
├── pyproject.toml    # Poetry configuration
└── README.md         # Project overview
```

## Development Tips

1. **Use the watch mode** (`task watch`) for active development
2. **Run tests frequently** to catch issues early
3. **Follow the existing code style** - let Ruff handle formatting
4. **Write descriptive commit messages**
5. **Keep PRs focused** on a single feature or fix
6. **Add tests** for new functionality
7. **Update documentation** when adding features

## License

By contributing to Cleared, you agree that your contributions will be licensed under the Apache License 2.0.

---

Thank you for contributing to Cleared! 🎉
