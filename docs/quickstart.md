# Quickstart Guide

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

## Next Steps

- Learn about [using configuration files](use_cleared_config.md) for more complex setups
- Explore [multi-table pipelines](multi_table_pipeline_config.md) for related data
- Check out the [API reference](../api/) for more transformer options