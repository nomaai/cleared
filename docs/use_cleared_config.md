# Using Cleared with Configuration Files

This guide shows you how to use Cleared with configuration files for the same de-identification scenario as the [Quickstart Guide](quickstart.md), but using structured configuration files and the Cleared CLI/Engine.

## Prerequisites

Make sure you have Cleared installed:

```bash
pip install cleared
```

## Setup Working Directory

Create a working directory with the required structure:

```bash
mkdir -p ~/cleared_config_tutorial/{input,output,deid_ref}
cd ~/cleared_config_tutorial
```

## 1. Create Sample Data

Create the input CSV file using Cleared's sample data:

```python
import cleared as clr

# Get sample users data and save to input directory
users_df = clr.sample_data.users_single_table
users_df.to_csv('input/users.csv', index=False)
print("Created input/users.csv")
```

**Sample Data Preview:**
| user_id | name           | reg_date_time        | zipcode |
|---------|----------------|---------------------|---------|
| 101     | Alice Johnson  | 2020-01-15 10:30:00 | 10001   |
| 202     | Bob Smith      | 2019-06-22 14:45:00 | 90210   |
| 303     | Charlie Brown  | 2021-03-08 09:15:00 | 60601   |
| 404     | Diana Prince   | 2018-11-12 16:20:00 | 33101   |
| 505     | Eve Wilson     | 2022-07-03 11:55:00 | 98101   |

## 2. Create Configuration File

Create a YAML configuration file `users_config.yaml` that replicates the exact same de-identification as the quickstart example:

```yaml
name: "users_deid_pipeline"
deid_config:
  time_shift:
    method: "shift_by_years"
    min: -5
    max: 5
io:
  data:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "./input"
        file_format: "csv"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./output"
        file_format: "csv"
  deid_ref:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "./deid_ref"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./deid_ref"
  runtime_io_path: "./runtime"
tables:
  users:
    name: "users"
    depends_on: []
    transformers:
      - method: "IDDeidentifier"
        uid: "user_id_transformer"
        depends_on: []
        configs:
          idconfig:
            name: "user_id"
            uid: "user_uid"
            description: "User identifier"
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        depends_on: ["user_id_transformer"]
        configs:
          idconfig:
            name: "user_id"
            uid: "user_uid"
            description: "User identifier"
          datetime_column: "reg_date_time"
      - method: "ColumnDropper"
        uid: "name_drop_transformer"
        depends_on: ["datetime_transformer"]
        configs:
          idconfig:
            name: "name"
            uid: "name_drop"
            description: "User name to drop"
```

## 3. Understanding the Configuration File

Let's break down every part of the configuration file:

### Pipeline Name
```yaml
name: "users_deid_pipeline"
```
This sets the name of the entire de-identification pipeline.

### De-identification Configuration
```yaml
deid_config:
  time_shift:
    method: "shift_by_years"
    min: -5
    max: 5
```
This configures datetime shifting to randomly shift dates by -5 to +5 years, exactly like the quickstart example.

### IO Configuration
```yaml
io:
  data:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "./input"
        file_format: "csv"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./output"
        file_format: "csv"
  deid_ref:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "./deid_ref"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./deid_ref"
  runtime_io_path: "./runtime"
```

- **`data.input_config`**: Reads CSV files from the `./input` directory
- **`data.output_config`**: Writes results to the `./output` directory  
- **`deid_ref`**: Stores de-identification reference tables in `./deid_ref` directory
- **`runtime_io_path`**: Temporary files during processing

### Table Configuration
```yaml
tables:
  users:
    name: "users"
    depends_on: []
    transformers:
      - method: "IDDeidentifier"
        uid: "user_id_transformer"
        depends_on: []
        configs:
          idconfig:
            name: "user_id"
            uid: "user_uid"
            description: "User identifier"
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        depends_on: ["user_id_transformer"]
        configs:
          idconfig:
            name: "user_id"
            uid: "user_uid"
            description: "User identifier"
          datetime_column: "reg_date_time"
      - method: "ColumnDropper"
        uid: "name_drop_transformer"
        depends_on: ["datetime_transformer"]
        configs:
          idconfig:
            name: "name"
            uid: "name_drop"
            description: "User name to drop"
```

This defines the `users` table with three transformers in sequence:
1. **IDDeidentifier**: De-identifies `user_id` column (same as quickstart)
2. **DateTimeDeidentifier**: Shifts `reg_date_time` column (same as quickstart)  
3. **ColumnDropper**: Drops `name` column (same as quickstart)

The `depends_on` field ensures proper execution order.

## 4. Run De-identification with CLI

Use the Cleared CLI to run the de-identification:

```bash
cleared run users_config.yaml
```

Expected output:
```
Loaded configuration: users_deid_pipeline
Processing table: users
  - Running transformer: user_id_transformer (IDDeidentifier)
  - Running transformer: datetime_transformer (DateTimeDeidentifier)
  - Running transformer: name_drop_transformer (ColumnDropper)
De-identification complete!

Results:
  users: (5, 4) -> (5, 3)
  Columns: ['user_id', 'reg_date_time', 'zipcode']
```

## 5. Run De-identification with Engine

Alternatively, use the Cleared Engine programmatically:

```python
import cleared as clr
from cleared.cli.utils import load_config_from_file

# Load configuration
config = load_config_from_file("users_config.yaml")

# Create table pipeline directly
table_pipeline = clr.TablePipeline(
    table_name="users",
    io_config=config.io.data,
    deid_config=config.deid_config
)

# Run de-identification
results = table_pipeline.run()

print("De-identification complete!")
print(f"users: {results['users'].shape}")
print(f"Columns: {list(results['users'].columns)}")
```

## 6. Verify Results

Check the output files:

```bash
# Check the de-identified data
cat output/users.csv

# Check the de-identification reference tables
ls deid_ref/
cat deid_ref/user_uid.csv
cat deid_ref/user_uid_shift.csv
```

Expected output:
```csv
user_id,reg_date_time,zipcode
2,2017-01-15 10:30:00,10001
4,2020-06-22 14:45:00,90210
5,2021-03-08 09:15:00,60601
1,2018-11-12 16:20:00,33101
3,2020-07-03 11:55:00,98101
```

The results should be identical to the quickstart example:
- **user_id**: De-identified to sequential integers (1, 2, 3, 4, 5)
- **reg_date_time**: Randomly shifted by -5 to +5 years
- **name**: Column dropped
- **zipcode**: Unchanged



## Next Tutorial

Continue to the next tutorial: [Review De-identification Configuration](use_describe.md) - Generate and review HTML configuration reports
