# Reversing De-identification

The reverse functionality allows you to restore original values from de-identified data using the de-identification reference mappings that were created during the initial de-identification process.

## Overview

When you run de-identification, Cleared creates reference mappings that store the relationship between original and de-identified values. The reverse process uses these mappings to restore the original data from the de-identified output.

## Command Line Usage

Use the `cleared reverse` command to reverse de-identification from the command line:

```bash
cleared reverse <config.yaml> -o <reverse_output_directory>
```

### Basic Example

```bash
cleared reverse config.yaml -o ./reversed_data
```

### With Options

```bash
cleared reverse config.yaml \
  -o ./reversed_data \
  --continue-on-error \
  --verbose
```

### Command Options

- `-o, --output`: **Required**. Directory path where reversed data will be written
- `--continue-on-error, -c`: Continue running remaining pipelines even if one fails
- `--create-dirs, -d`: Create missing directories automatically
- `--verbose, -v`: Enable verbose output
- `--config-name, -cn`: Name of the configuration to load (default: `cleared_config`)
- `--override`: Override configuration values

## Programmatic Usage

You can also reverse de-identification programmatically using the `ClearedEngine` class:

### Basic Reverse

```python
from cleared.engine import ClearedEngine
from cleared.config.loader import load_config

# Load configuration
config = load_config("config.yaml")
engine = ClearedEngine.from_config(config)

# Run reverse
result = engine.run(
    reverse=True,
    reverse_output_path="./reversed_data"
)
```

### Reverse with Options

```python
# Reverse with error handling and test mode
result = engine.run(
    reverse=True,
    reverse_output_path="./reversed_data",
    continue_on_error=True,
    test_mode=False,  # Set to True for dry run
    rows_limit=None   # Optional: limit rows for testing
)
```

## How It Works

1. **Read De-identified Data**: The reverse process reads data from the output configuration (where de-identified data was written)

2. **Load Reference Mappings**: It loads the de-identification reference dictionaries that contain the mappings between original and de-identified values

3. **Apply Reverse Transformations**: Each transformer's `reverse()` method is called in reverse order to restore original values

4. **Write Reversed Data**: The restored data is written to the specified reverse output directory

## Important Notes

- **Reference Mappings Required**: The reverse process requires the de-identification reference files that were created during the initial de-identification run
- **Output Path Required**: You must specify a `reverse_output_path` when running in reverse mode
- **Transformer Support**: Only transformers that implement the `reverse()` method can be reversed
- **Order Matters**: Transformers are reversed in the opposite order they were applied during de-identification

> ⚠️ **Warning**: The `ColumnDropper` transformer is **not reversible**. Once a column is dropped during de-identification, it cannot be restored during the reverse process. If you need to preserve columns for reversal, avoid using `ColumnDropper` or ensure the original data is stored separately.

## Use Cases

- **Data Recovery**: Restore original values from de-identified datasets
- **Testing**: Verify that de-identification can be reversed correctly
- **Data Updates**: Apply updates to original data and re-run de-identification
- **Audit Trails**: Maintain ability to trace back to original data when needed

