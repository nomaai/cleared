.ONESHELL:

VENV=.venv
PROJECT_DIR_NAME=$(shell basename $(CURDIR))
ENV_PREFIX=$(shell python -c "if __import__('pathlib').Path('$(VENV)/bin/pip').exists(): print('$(VENV)/bin/')")

.PHONY: help
help:             ## Show the help.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@fgrep "##" Makefile | fgrep -v fgrep

.PHONY: env_path
env_path:
	@echo $(ENV_PREFIX)

.PHONY: show
show:             ## Show the current environment.
	@echo "Current environment:"
	@echo "Running using $(ENV_PREFIX)"
	@$(ENV_PREFIX)python -V
	@$(ENV_PREFIX)python -m site


.PHONY: install
install:          ## Install the project in dev mode.
	@echo "Don't forget to run 'make virtualenv' if you got errors."
	$(ENV_PREFIX)pip install -e .[dev]


.PHONY: lint
lint:          ## Run linter using ruff
	@$(ENV_PREFIX)ruff check --preview

.PHONY: lint_fix
lint_fix:         ## Run lint fixer using ruff
	@$(ENV_PREFIX)ruff check --preview


.PHONY: format
format:         ## Run formatter (black like style) using ruff
	@$(ENV_PREFIX)ruff format --preview


.PHONY: test
test:          ## Run formatter (black like style) using ruff
	@. scripts/run_pytest_ignore_error_5.sh

.PHONY: watch
watch:          ## Run tests on every change.
	ls **/**.py | entr $(ENV_PREFIX)pytest -s -vvv -l --tb=long --maxfail=1 tests/

.PHONY: clean
clean:          ## Clean unused files.
	@find ./ -name '*.pyc' -exec rm -f {} \;
	@find ./ -name '__pycache__' -exec rm -rf {} \;
	@find ./ -name 'Thumbs.db' -exec rm -f {} \;
	@find ./ -name '*~' -exec rm -f {} \;
	@rm -rf .cache
	@rm -rf .pytest_cache
	@rm -rf .mypy_cache
	@rm -rf build
	@rm -rf dist
	@rm -rf *.egg-info
	@rm -rf .tox/
	@rm -rf docs/_build
	@rm -rf .ruff_cache
	@rm -rf pytest-coverage.txt
	@rm -rf pytest.xml

.PHONY: init
init:   # initial project from template the first time.
	@. scripts/init_project.sh $(project_name) $(github_url)
	@echo


.PHONY: setup_env
setup_env:      ##Setup virtualenv and client git configs for CI/CD
	# Complete git configs
	@echo "Setting up environment"
	@command -v python3 >/dev/null 2>&1 || { echo >&2 "Error: python3 is not installed."; exit 1; }
	@rm -rf $(VENV)
	@python3 -m venv $(VENV) || { echo >&2 "Error: python3-venv is not installed."; exit 1; }
	@./$(VENV)/bin/pip install setuptools
	@./$(VENV)/bin/pip install -U pip
	@./$(VENV)/bin/pip install -e ".[dev]"
	@echo "Configuring pre-commit to enable local CI/CD hooks"
	@./$(VENV)/bin/pre-commit install --hook-type pre-commit --hook-type pre-push
	@echo
	@echo "!!! Please activate the environment !!!"

.PHONY: install_jupyter
install_jupyter:  ## install jupyterlab
	@./$(VENV)/bin/python -m ipykernel install --user --name "$(PROJECT_DIR_NAME)_$(ENV)" --display-name "$(PROJECT_DIR_NAME) $(ENV)"

	# Create a jupytur lab config file.
	@./$(VENV)/bin/jupyter server --generate-config

	# configure a jupyter lab password
	@echo "Configuring Jupyterlab server requires choosing a password"
	@./$(VENV)/bin/jupyter server password

.PHONY: run_jupyter
run_jupyter: # Run jupyterlab for remote access
	. scripts/run_jupyter_remote.sh

.PHONY: release
release:          ## Create a new tag for release.
	@echo "WARNING: This operation will create s version tag and push to github"
	@read -p "Version? (provide the next x.y.z semver) : " TAG
	@echo "${TAG}" > VERSION
	@$(ENV_PREFIX)gitchangelog > HISTORY.md
	@git add VERSION HISTORY.md
	@git commit -m "release: version ${TAG} 🚀"
	@echo "creating git tag : ${TAG}"
	@git tag $${TAG}
	@git push -u origin HEAD --tags
	@echo "Github Actions will detect the new tag and release the new version."

.PHONY: docs
docs:             ## Build the documentation.
	@echo "building documentation ..."
	@$(ENV_PREFIX)mkdocs build
	URL="site/index.html"; xdg-open $$URL || sensible-browser $$URL || x-www-browser $$URL || gnome-open $$URL || open $$URL
