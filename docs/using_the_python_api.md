# Using the Python API

This guide shows you how to get started with Cleared for de-identifying a single table using transformers directly.

## Prerequisites

Make sure you have Cleared installed:

```bash
pip install cleared
```

## Basic Single Table De-identification

Let's start with a simple example of de-identifying a users table with ID de-identification, datetime shifting, and column dropping.

### 1. Import Required Modules

```python
import pandas as pd
from datetime import datetime
import cleared as clr
```

### 2. Create Sample Data

```python
# Get sample users data
users_df = clr.sample_data.users_single_table

print("Original data:")
print(users_df)
```

**Sample Data Preview:**
| user_id | name           | reg_date_time        | zipcode |
|---------|----------------|---------------------|---------|
| 101     | Alice Johnson  | 2020-01-15 10:30:00 | 10001   |
| 202     | Bob Smith      | 2019-06-22 14:45:00 | 90210   |
| 303     | Charlie Brown  | 2021-03-08 09:15:00 | 60601   |
| 404     | Diana Prince   | 2018-11-12 16:20:00 | 33101   |
| 505     | Eve Wilson     | 2022-07-03 11:55:00 | 98101   |

### 3. Configure De-identification

```python
# Configure user ID de-identification
user_id_config = clr.IdentifierConfig(
    name="user_id",
    uid="user_uid",
    description="User identifier"
)

# Configure datetime shifting (random years between -5 and 5)
time_shift_config = clr.TimeShiftConfig(
    method="shift_by_years",
    min=-5,
    max=5
)

deid_config = clr.DeIDConfig(
    time_shift=time_shift_config
)
```

### 4. Create TablePipeline

```python
# Create transformers for the pipeline
transformers = [
    clr.IDDeidentifier(idconfig=user_id_config),
    clr.DateTimeDeidentifier(
        idconfig=user_id_config,
        deid_config=deid_config,
        datetime_column="reg_date_time"
    ),
    clr.ColumnDropper(
        idconfig=clr.IdentifierConfig(
            name="name",
            uid="name_drop",
            description="User name to drop"
        )
    )
]

# Create a simple IO config (not used since we're providing data directly)
io_config = clr.PairedIOConfig(
    input_config=clr.IOConfig(io_type="filesystem", configs={"base_path": "."}),
    output_config=clr.IOConfig(io_type="filesystem", configs={"base_path": "."})
)

# Create table pipeline
table_pipeline = clr.TablePipeline(
    table_name="users",
    io_config=io_config,
    deid_config=deid_config,
    transformers=transformers
)
```

### 5. Apply Transformations

```python
# Run the pipeline with the input data
# The pipeline will process the DataFrame without reading/writing files
users_df_deid, deid_ref_dict = table_pipeline.transform(users_df)

print("\nDe-identified data:")
print(users_df_deid)
```

### 6. Verify Results

```python
# Check that transformations worked
print(f"Original user IDs: {sorted(users_df['user_id'].unique())}")
print(f"De-identified user IDs: {sorted(users_df_deid['user_id'].unique())}")

# Check that name column was dropped
print(f"Columns after transformation: {list(users_df_deid.columns)}")

# Check that dates were shifted
print(f"Original dates: {users_df['reg_date_time'].head()}")
print(f"Shifted dates: {users_df_deid['reg_date_time'].head()}")
```

### 7. Access De-identification Mappings

```python
# Access the ID mapping
id_mapping = deid_ref_dict.get("user_uid")
print("\nID Mapping:")
print(id_mapping)

# Access the time shift mapping
time_shift_mapping = deid_ref_dict.get("user_uid_shift")
print("\nTime Shift Mapping:")
print(time_shift_mapping)
```

## Complete Example

Here's the complete script:

```python
import cleared as clr

# Get sample data
users_df = clr.sample_data.users_single_table

# Configure de-identification
user_id_config = clr.IdentifierConfig(
    name="user_id",
    uid="user_uid",
    description="User identifier"
)

time_shift_config = clr.TimeShiftConfig(
    method="shift_by_years",
    min=-5,
    max=5
)

deid_config = clr.DeIDConfig(time_shift=time_shift_config)

# Create transformers for the pipeline
transformers = [
    clr.IDDeidentifier(idconfig=user_id_config),
    clr.DateTimeDeidentifier(
        idconfig=user_id_config,
        deid_config=deid_config,
        datetime_column="reg_date_time"
    ),
    clr.ColumnDropper(
        idconfig=clr.IdentifierConfig(
            name="name",
            uid="name_drop",
            description="User name to drop"
        )
    )
]

# Create a simple IO config (not used since we're providing data directly)
io_config = clr.PairedIOConfig(
    input_config=clr.IOConfig(io_type="filesystem", configs={"base_path": "."}),
    output_config=clr.IOConfig(io_type="filesystem", configs={"base_path": "."})
)

# Create table pipeline
table_pipeline = clr.TablePipeline(
    table_name="users",
    io_config=io_config,
    deid_config=deid_config,
    transformers=transformers
)

# Apply transformations
users_df_deid, deid_ref_dict = table_pipeline.transform(users_df)

print("De-identification complete!")
print(f"Original shape: {users_df.shape}")
print(f"De-identified shape: {users_df_deid.shape}")
print(f"Columns: {list(users_df_deid.columns)}")
```

## Testing Your Configuration

Before running your configuration on the full dataset, you can test it with a limited number of rows using the `cleared test` command. This performs a dry run that processes only the first N rows of each table and does not write any outputs, making it safe to test your configuration:

```bash
# Test with default 10 rows per table
cleared test config.yaml

# Test with more rows
cleared test config.yaml --rows 50
```

The test command runs the same process as `cleared run` but:
- Only processes the first N rows of each table (configurable with `--rows`)
- Does not write any output files (dry run mode)
- Validates that your configuration works correctly without modifying data

This is especially useful for:
- Verifying your transformers work as expected
- Testing complex configurations before full runs
- Debugging configuration issues with a small sample

## Viewing Configuration Details

Before running your configuration, you can generate a visual HTML report to review all configuration details:

```bash
# Generate an HTML report of your configuration
cleared describe config.yaml

# Or specify a custom output file
cleared describe config.yaml -o my_config_report.html
```

The report includes:
- Overview statistics (tables, transformers, dependencies)
- De-identification configuration details
- I/O configuration settings
- Detailed transformer information for each table
- Interactive features (sorting, filtering, PDF export)

Open the generated HTML file in your browser to view the comprehensive configuration report.

## Validating Configuration Files

If you're using configuration files (YAML) instead of programmatic setup, you can validate your configuration before running the de-identification process using the `cleared validate` command.

### Validate a Configuration File

The `cleared validate` command performs comprehensive validation by:
1. **Checking configuration syntax** - Verifies the configuration can be loaded and initialized
2. **Linting the configuration** - Performs YAML syntax checking and Cleared-specific rule validation

```bash
# Validate a configuration file
cleared validate config.yaml

# Validate with strict mode (treats warnings as errors)
cleared validate config.yaml --strict

# Validate with verbose output
cleared validate config.yaml --verbose
```

**Example Output:**
```bash
$ cleared validate config.yaml

🔍 Step 1: Checking configuration syntax...
Configuration loaded from: config.yaml
✅ Configuration is valid!
Engine would be initialized with 1 pipelines

✅ Syntax check passed

🔍 Step 2: Linting configuration...

📋 Running YAML linting (yamllint)...
  ✅ No YAML syntax issues found

🔍 Running Cleared-specific linting...
  ✅ No Cleared-specific issues found

============================================================
✅ Validation completed successfully!
============================================================
```

If there are any issues, the command will report them with specific rule IDs and line numbers. For a complete reference of all linting rules, see the [Linting Rules Reference](linting_rules.md).

### Other Validation Commands

You can also use more specific validation commands:

```bash
# Check syntax only (without linting)
cleared check-syntax config.yaml

# Lint only (without syntax checking)
cleared lint config.yaml
```

## Next Tutorial

Continue to the next tutorial: [Free-text PHI Detection](phi-detection.md)
