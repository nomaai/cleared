# CLI Usage Guide

The Cleared framework provides a powerful command-line interface (CLI) that allows you to run data de-identification processes, validate configurations, and manage projects directly from the terminal.

## Table of Contents

- [Installation](#installation)
- [Available Commands](#available-commands)
  - [`cleared run`](#cleared-run)
  - [`cleared test`](#cleared-test)
  - [`cleared validate`](#cleared-validate)
  - [`cleared check-syntax`](#cleared-check-syntax)
  - [`cleared lint`](#cleared-lint)
  - [`cleared init`](#cleared-init)
  - [`cleared setup`](#cleared-setup)
  - [`cleared describe`](#cleared-describe)
  - [`cleared info`](#cleared-info)
- [Configuration File Format](#configuration-file-format)
- [Hydra Integration](#hydra-integration)
- [Getting Started](#getting-started)
- [Output and Results](#output-and-results)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)
- [Advanced Usage](#advanced-usage)
  - [Custom Transformers](#custom-transformers)
  - [Batch Processing](#batch-processing)
  - [Integration with CI/CD](#integration-with-cicd)
- [Troubleshooting](#troubleshooting)
  - [Debug Mode](#debug-mode)
  - [Configuration Validation](#configuration-validation)
  - [Log Files](#log-files)
- [Support](#support)

## Installation

The CLI is automatically available when you install the Cleared package:

```bash
poetry install
```

## Available Commands

### `cleared run`

Run the ClearedEngine with a configuration file.

**Usage:**
```bash
cleared run <config.yaml> [OPTIONS]
```

**Arguments:**
- `config.yaml` - Path to the configuration file

**Options:**
- `--create-dirs`, `-d` - Create missing directories automatically
- `--continue-on-error`, `-c` - Continue running remaining pipelines even if one fails
- `--verbose`, `-v` - Enable verbose output

**Examples:**
```bash
# Basic usage
cleared run config.yaml

# Create missing directories automatically
cleared run config.yaml --create-dirs

# Continue on error with verbose output
cleared run config.yaml --continue-on-error --verbose

# Run with Hydra-style overrides
cleared run config.yaml -o "io.data.input_config.configs.base_path=/custom/path"
```

### `cleared test`

Test run the ClearedEngine with a limited number of rows (dry run).

**Usage:**
```bash
cleared test <config.yaml> [OPTIONS]
```

**Arguments:**
- `config.yaml` - Path to the configuration file

**Options:**
- `--rows`, `-r` - Number of rows to process per table (default: 10)
- `--config-name`, `-cn` - Name of the configuration to load (default: cleared_config)
- `--override` - Override configuration values
- `--continue-on-error`, `-c` - Continue running remaining pipelines even if one fails
- `--create-dirs`, `-d` - Create missing directories automatically
- `--verbose`, `-v` - Enable verbose output

**Description:**
This command runs the same process as `cleared run` but only processes the first N rows of each table and does not write any outputs. This is useful for testing your configuration before running on the full dataset. It's a dry run mode that validates your configuration works correctly without modifying any files.

**Examples:**
```bash
# Test with default 10 rows per table
cleared test config.yaml

# Test with 50 rows per table
cleared test config.yaml --rows 50

# Test with verbose output
cleared test config.yaml -r 100 --verbose

# Test with overrides
cleared test config.yaml -r 20 -o "name=test"
```

### `cleared validate`

Comprehensively validate a configuration file (runs check-syntax and lint).

**Usage:**
```bash
cleared validate <config.yaml> [OPTIONS]
```

**Arguments:**
- `config.yaml` - Path to the configuration file to validate

**Options:**
- `--config-name`, `-cn` - Name of the configuration to load (default: cleared_config)
- `--override`, `-o` - Override configuration values for validation
- `--check-paths` - Check if required paths exist (default: true)
- `--yamllint-config` - Path to yamllint configuration file (default: .yamllint)
- `--strict`, `-s` - Treat warnings as errors
- `--verbose`, `-v` - Enable verbose output

**Description:**
This command performs comprehensive validation by:
1. **Checking configuration syntax** - Verifies the configuration can be loaded and initialized
2. **Linting the configuration** - Performs YAML syntax checking and Cleared-specific rule validation

**Examples:**
```bash
# Full validation (syntax + lint)
cleared validate config.yaml

# Validate with strict mode (warnings as errors)
cleared validate config.yaml --strict

# Validate with overrides
cleared validate config.yaml -o "name=test" --verbose

# Validate without checking paths
cleared validate config.yaml --no-check-paths
```

### `cleared check-syntax`

Check configuration file syntax and structure without running the engine.

**Usage:**
```bash
cleared check-syntax <config.yaml> [OPTIONS]
```

**Arguments:**
- `config.yaml` - Path to the configuration file to check

**Options:**
- `--config-name`, `-cn` - Name of the configuration to load (default: cleared_config)
- `--override`, `-o` - Override configuration values for checking
- `--check-paths` - Check if required paths exist (default: true)

**Description:**
This command loads and checks a configuration file to verify it can be loaded and initialized before running the actual de-identification process. It does not perform linting.

**Examples:**
```bash
# Check configuration syntax
cleared check-syntax config.yaml

# Check syntax with overrides
cleared check-syntax config.yaml -o "name=test"

# Check syntax without checking paths
cleared check-syntax config.yaml --no-check-paths
```

### `cleared lint`

Lint a configuration file (YAML syntax + Cleared-specific rules).

**Usage:**
```bash
cleared lint <config.yaml> [OPTIONS]
```

**Arguments:**
- `config.yaml` - Path to the configuration file to lint

**Options:**
- `--config-name`, `-cn` - Name of the configuration to load (default: cleared_config)
- `--override`, `-o` - Override configuration values before linting
- `--yamllint-config` - Path to yamllint configuration file (default: .yamllint)
- `--strict`, `-s` - Treat warnings as errors
- `--verbose`, `-v` - Enable verbose output

**Description:**
This command performs both YAML syntax/structure linting (using yamllint) and Cleared-specific configuration linting (custom rules). It does not check if the configuration can be loaded or initialized.

For a complete reference of all available linting rules, see the [Linting Rules Reference](linting_rules.md).

**Examples:**
```bash
# Lint configuration
cleared lint config.yaml

# Lint with strict mode
cleared lint config.yaml --strict

# Lint with custom yamllint config
cleared lint config.yaml --yamllint-config .custom-yamllint

# Lint with overrides
cleared lint config.yaml -o "name=test" --verbose
```

### `cleared init`

Initialize a new Cleared project with a sample configuration file.

**Usage:**
```bash
cleared init [output.yaml] [OPTIONS]
```

**Arguments:**
- `output.yaml` - Path where to create the sample configuration file (default: sample_config.yaml)

**Options:**
- `--force`, `-f` - Overwrite existing file if it exists

**Examples:**
```bash
# Create default sample config
cleared init

# Create custom sample config
cleared init my_project_config.yaml

# Overwrite existing file
cleared init config.yaml --force
```

### `cleared setup`

Create project directories based on the configuration file.

**Usage:**
```bash
cleared setup <config.yaml> [OPTIONS]
```

**Arguments:**
- `config.yaml` - Path to the configuration file

**Options:**
- `--config-name`, `-cn` - Name of the configuration to load (default: cleared_config)
- `--override`, `-o` - Override configuration values before creating directories
- `--verbose`, `-v` - Enable verbose output

**Description:**
This command reads the configuration file and creates all required directories specified in the IO configuration (input paths, output paths, deid_ref paths, and runtime path). Directories that already exist are skipped.

**Examples:**
```bash
# Create all directories from config
cleared setup config.yaml

# Create directories with overrides
cleared setup config.yaml -o "io.data.input_config.configs.base_path=/custom/path"

# Verbose output
cleared setup config.yaml --verbose
```

### `cleared describe`

Generate an HTML report describing the Cleared configuration.

**Usage:**
```bash
cleared describe <config.yaml> [OPTIONS]
```

**Arguments:**
- `config.yaml` - Path to the configuration file

**Options:**
- `--config-name`, `-cn` - Name of the configuration to load (default: cleared_config)
- `--override` - Override configuration values before generating report
- `--output`, `-o` - Output HTML file path (default: describe.html in current directory)
- `--verbose`, `-v` - Enable verbose output

**Description:**
This command loads a configuration file and generates a comprehensive HTML report with all configuration details, including:
- Overview statistics (table count, transformer count, dependency count)
- De-identification configuration (time shift method and range)
- I/O configuration (input/output paths, file formats, storage types)
- Tables section with detailed transformer information
- Interactive features (sortable/filterable tables, PDF export)

**Examples:**
```bash
# Generate HTML report (saves to describe.html)
cleared describe config.yaml

# Generate report with custom output path
cleared describe config.yaml -o report.html

# Generate report with overrides
cleared describe config.yaml -o report.html --override "name=test" --verbose
```

### `cleared info`

Show information about the Cleared framework.

**Usage:**
```bash
cleared info
```

**Examples:**
```bash
# Show framework information
cleared info
```

## Configuration File Format

The CLI uses YAML configuration files that define your de-identification setup. Here's a sample configuration:

```yaml
# Engine name
name: "my_deid_engine"

# De-identification configuration
deid_config:
  time_shift:
    method: "random_days"
    min: -365
    max: 365

# I/O configuration
io:
  data:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "/path/to/input"
        file_format: "csv"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "/path/to/output"
        file_format: "csv"
  
  deid_ref:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "/path/to/deid_ref_input"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "/path/to/deid_ref_output"
  
  runtime_io_path: "/path/to/runtime"

# Table configurations
tables:
  patients:
    name: "patients"
    depends_on: []
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        depends_on: []
        configs:
          idconfig:
            name: "patient_id"
            uid: "patient_id"
            description: "Patient identifier"
  
  encounters:
    name: "encounters"
    depends_on: ["patients"]
    transformers:
      - method: "IDDeidentifier"
        uid: "encounter_id_transformer"
        depends_on: []
        configs:
          idconfig:
            name: "encounter_id"
            uid: "encounter_id"
            description: "Encounter identifier"
      
      - method: "DateTimeDeidentifier"
        uid: "admission_date_transformer"
        depends_on: ["encounter_id_transformer"]
        configs:
          idconfig:
            name: "patient_id"
            uid: "patient_id"
            description: "Patient identifier for time shifting"
          datetime_column: "admission_date"
```

## Hydra Integration

The CLI supports Hydra's powerful configuration override system, allowing you to modify configuration values from the command line:

```bash
# Override specific configuration values
cleared run config.yaml -o "io.data.input_config.configs.base_path=/new/path"
cleared run config.yaml -o "deid_config.time_shift.method=shift_by_years"
cleared run config.yaml -o "tables.patients.transformers[0].configs.idconfig.name=custom_id"

# Multiple overrides
cleared run config.yaml -o "io.data.input_config.configs.base_path=/input" -o "io.data.output_config.configs.base_path=/output"
```

## Getting Started

1. **Initialize a new project:**
   ```bash
   cleared init my_project.yaml
   ```

2. **Edit the configuration file:**
   ```bash
   nano my_project.yaml
   ```

3. **Validate your configuration:**
   ```bash
   cleared validate my_project.yaml
   ```

4. **Test your configuration (recommended):**
   ```bash
   cleared test my_project.yaml
   ```
   This runs a dry run with the first 10 rows to verify everything works before processing the full dataset.

5. **Generate a configuration report (optional):**
   ```bash
   cleared describe my_project.yaml
   ```

6. **Run the de-identification:**
   ```bash
   cleared run my_project.yaml --create-dirs
   ```

## Output and Results

When you run the de-identification process, the CLI will:

1. **Load and validate** your configuration
2. **Create missing directories** (if `--create-dirs` is used)
3. **Initialize the engine** with your pipelines
4. **Process each table** according to your configuration
5. **Save results** to the runtime directory
6. **Display status** of the execution

### Example Output

```bash
$ cleared run config.yaml --verbose

Configuration loaded from: config.yaml
create_dirs flag: True
missing_paths: ['data_input', 'data_output', 'deid_ref_input', 'deid_ref_output', 'runtime']
Creating missing directories...
Created directory: /path/to/input
Created directory: /path/to/output
Created directory: /path/to/deid_ref_input
Created directory: /path/to/deid_ref_output
Created directory: /path/to/runtime
Engine initialized with 2 pipelines
  Pipeline 1: 3c886591-0453-4d3e-b398-8ab66809c811
  Pipeline 2: b5ab243c-bc9b-4a25-98b2-7180f95d3e4e
Starting de-identification process...
✅ De-identification completed successfully!

Pipeline Results:
  ✅ 3c886591-0453-4d3e-b398-8ab66809c811: success
  ✅ b5ab243c-bc9b-4a25-98b2-7180f95d3e4e: success
```

## Error Handling

The CLI provides comprehensive error handling:

- **Configuration errors**: Clear messages about invalid configuration
- **File not found**: Helpful suggestions for missing files or directories
- **Pipeline errors**: Detailed error messages with context
- **Validation errors**: Specific information about what needs to be fixed

### Common Issues and Solutions

**Missing directories:**
```bash
# Solution: Use --create-dirs flag
cleared run config.yaml --create-dirs
```

**Invalid configuration:**
```bash
# Solution: Validate first, then fix issues
cleared validate config.yaml

# Or check syntax and lint separately
cleared check-syntax config.yaml
cleared lint config.yaml
```

**Pipeline execution errors:**
```bash
# Solution: Use --continue-on-error to see all errors
cleared run config.yaml --continue-on-error --verbose
```

## Best Practices

1. **Always validate** your configuration before running
2. **Use version control** for your configuration files
3. **Test with small datasets** before processing large amounts of data
4. **Use verbose output** when debugging issues
5. **Keep backups** of your original data
6. **Document your configuration** with comments in the YAML file

## Advanced Usage

### Custom Transformers

You can extend the CLI with custom transformers by modifying your configuration:

```yaml
tables:
  my_table:
    name: "my_table"
    transformers:
      - method: "MyCustomTransformer"
        uid: "custom_transformer"
        configs:
          custom_param: "value"
```

### Batch Processing

For processing multiple datasets, you can use shell scripts:

```bash
#!/bin/bash
for config in examples/*.yaml; do
    echo "Processing $config"
    cleared run "$config" --create-dirs
done
```

### Integration with CI/CD

The CLI can be integrated into automated workflows:

```yaml
# GitHub Actions example
- name: Run de-identification
  run: |
    cleared validate config.yaml
    cleared run config.yaml --create-dirs
```

## Troubleshooting

### Debug Mode

For detailed debugging information, use the verbose flag:

```bash
cleared run config.yaml --verbose
```

### Configuration Validation

Always validate your configuration before running:

```bash
# Full validation (recommended)
cleared validate config.yaml

# Or check syntax and lint separately
cleared check-syntax config.yaml --check-paths
cleared lint config.yaml
```

### Log Files

Results and execution logs are saved to the runtime directory specified in your configuration.

## Support

For additional help and support:

- Check the [main documentation](../README.md)
- Review the [contributing guide](contributing.md)
- Open an issue on the [GitHub repository](https://github.com/nomaai/cleared)
