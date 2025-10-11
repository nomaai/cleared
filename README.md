# noma_python_template

## When to use this template?
Use this template when you are creating a pure python project or python backend only project. Examples include: data science, machine learning, api-only backends, etc.

## How to use this template?

> **DO NOT FORK** read the how use this template to docs first: [how to use this template](docs/template/use_this_template.md)


## What is included on this template?


- 📦 A basic [setup.py](setup.py) file to provide installation, packaging and distribution for your project.  
  Template uses setuptools because it's the de-facto standard for Python packages, you can run `make switch-to-poetry` later if you want.
- 💬 Auto generation of change log using **gitchangelog** to keep a HISTORY.md file automatically based on your commit history on every release.
- 🔄 Continuous integration using [Github Actions](.github/workflows/) with jobs to lint, test and release your project on Linux, Mac and Windows environments.
- Git Hooks and configuration to run CI pipeline localy as a requirement for rapid feedback
- Configurations to run jupyter lab for your project from any server
  - CI Pipeline:
    - Linting and formatting: ruff: A super fast linter and formatter written in Rust
    - Testing: pytest
    - type checker: pyright # to be added