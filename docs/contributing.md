
# Contributing
```bash
❯ make
Usage: make <target>

Targets:
help:             ## Show the help.
show:             ## Show the current environment.
install:          ## Install the project in dev mode.
lint:             ## Run linter using ruff
lint_fix:         ## Run lint fixer using ruff
init              ## initialize the project. Can only be run once.
format:           ## Run formatter (black like style) using ruff
test:             ## Run formatter (black like style) using ruff
watch:            ## Run tests on every change.
clean:            ## Clean unused files.
setup_env:        ##Setup virtualenv and client git configs for CI/CD
install_jupyter:  ## install jupyterlab
release:          ## Create a new tag for release.
docs:             ## Build the documentation.

```