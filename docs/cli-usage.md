# CLI Usage Guide

The Cleared framework provides a powerful command-line interface (CLI) that allows you to run data de-identification processes, validate configurations, and manage projects directly from the terminal.

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

### `cleared validate`

Validate a configuration file without running the engine.

**Usage:**
```bash
cleared validate <config.yaml> [OPTIONS]
```

**Arguments:**
- `config.yaml` - Path to the configuration file to validate

**Options:**
- `--check-paths` - Check if required paths exist (default: true)

**Examples:**
```bash
# Validate configuration
cleared validate config.yaml

# Validate without checking paths
cleared validate config.yaml --no-check-paths
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
  global_uids:
    patient_id:
      name: "patient_id"
      uid: "patient_id"
      description: "Patient identifier"
    encounter_id:
      name: "encounter_id"
      uid: "encounter_id"
      description: "Encounter identifier"
  
  time_shift:
    method: "random_days"
    ref_id: "patient_id"
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
          time_shift_method: "random_days"
          datetime_column: "admission_date"
```

## Hydra Integration

The CLI supports Hydra's powerful configuration override system, allowing you to modify configuration values from the command line:

```bash
# Override specific configuration values
cleared run config.yaml -o "io.data.input_config.configs.base_path=/new/path"
cleared run config.yaml -o "deid_config.global_uids.patient_id.name=custom_patient_id"
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

4. **Run the de-identification:**
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
cleared validate config.yaml --check-paths
```

### Log Files

Results and execution logs are saved to the runtime directory specified in your configuration.

## Support

For additional help and support:

- Check the [main documentation](../README.md)
- Review the [contributing guide](contributing.md)
- Open an issue on the [GitHub repository](https://github.com/nomaai/cleared)
