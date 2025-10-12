.ONESHELL:

PROJECT_DIR_NAME=$(shell basename $(CURDIR))

.PHONY: help
help:             ## Show the help.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@fgrep "##" Makefile | fgrep -v fgrep

.PHONY: show
show:             ## Show the current environment.
	@echo "Current environment:"
	@poetry env info
	@poetry run python -V

.PHONY: install
install:          ## Install the project in dev mode.
	@echo "Installing project with Poetry..."
	@poetry install


.PHONY: lint
lint:          ## Run linter using ruff
	@poetry run ruff check

.PHONY: lint_fix
lint_fix:         ## Run lint fixer using ruff
	@poetry run ruff check --fix

.PHONY: format
format:         ## Run formatter (black like style) using ruff
	@poetry run ruff format

.PHONY: test
test:          ## Run tests using pytest
	@poetry run pytest

.PHONY: watch
watch:          ## Run tests on every change.
	ls **/**.py | entr poetry run pytest -s -vvv -l --tb=long --maxfail=1 tests/

.PHONY: clean
clean:          ## Clean unused files.
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@find ./ -name '__pycache__' -exec rm -rf {} \;
	@find ./ -name 'Thumbs.db' -exec rm -f {} \;
	@find ./ -name '*~' -exec rm -f {} \;
	@rm -rf .cache
	@rm -rf .pytest_cache
	@rm -rf .mypy_cache
	@rm -rf dist
	@rm -rf .tox/
	@rm -rf docs/_build
	@rm -rf .ruff_cache
	@rm -rf pytest-coverage.txt
	@rm -rf pytest.xml
	@poetry cache clear --all .

.PHONY: init
init:   # initial project from template the first time.
	@. scripts/init_project.sh $(project_name) $(github_url)
	@echo


.PHONY: setup_env
setup_env:      ##Setup Poetry environment and git configs for CI/CD
	# Complete git configs
	@echo "Setting up environment with Poetry"
	@command -v poetry >/dev/null 2>&1 || { echo >&2 "Error: Poetry is not installed. Please install Poetry first."; exit 1; }
	@poetry install
	@echo "Configuring pre-commit to enable local CI/CD hooks"
	@poetry run pre-commit install --hook-type pre-commit --hook-type pre-push
	@echo
	@echo "Environment setup complete!"

.PHONY: install_jupyter
install_jupyter:  ## install jupyterlab
	@poetry run python -m ipykernel install --user --name "$(PROJECT_DIR_NAME)" --display-name "$(PROJECT_DIR_NAME)"

	# Create a jupyter lab config file.
	@poetry run jupyter server --generate-config

	# configure a jupyter lab password
	@echo "Configuring Jupyterlab server requires choosing a password"
	@poetry run jupyter server password

.PHONY: run_jupyter
run_jupyter: # Run jupyterlab for remote access
	. scripts/run_jupyter_remote.sh

.PHONY: release
release:          ## Create a new tag for release.
	@echo "WARNING: This operation will create a version tag and push to github"
	@read -p "Version? (provide the next x.y.z semver) : " TAG
	@poetry version $${TAG}
	@poetry run gitchangelog > HISTORY.md
	@git add pyproject.toml HISTORY.md
	@git commit -m "release: version ${TAG} 🚀"
	@echo "creating git tag : ${TAG}"
	@git tag $${TAG}
	@git push -u origin HEAD --tags
	@echo "Github Actions will detect the new tag and release the new version."

.PHONY: docs
docs:             ## Build the documentation.
	@echo "building documentation ..."
	@poetry run mkdocs build
	URL="site/index.html"; xdg-open $$URL || sensible-browser $$URL || x-www-browser $$URL || gnome-open $$URL || open $$URL
