# Filtered De-identification Example

This guide demonstrates how to use Cleared's filtered de-identification capabilities to selectively de-identify data based on specific conditions. This is particularly useful when you want to de-identify only certain types of events or data while preserving others.

In this tutorial, we'll de-identify survey submission dates by filtering for "Survey submission date" events and de-identifying both the event timestamp and the survey submission date value.

## Prerequisites

**It is highly recommended to review the [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md) tutorial first**, as this tutorial builds upon the concepts and configuration structure introduced there. Some sections of this tutorial reference code and configurations that are explained in detail in the multi-table tutorial.

Make sure you have Cleared installed:

```bash
pip install cleared
```

## Setup Working Directory

Create a working directory with the required structure:

```bash
mkdir -p ~/cleared_filter_deid/{input,output,deid_ref}
cd ~/cleared_filter_deid
```

## 1. Create Sample Data

Create CSV files for two related tables using Cleared's sample data:

```python
import cleared as clr

# Get sample users dataset
users_df = clr.sample_data.users_multi_table
users_df.to_csv('input/users.csv', index=False)
print(f"Created input/users.csv: {users_df.shape}")

# Get sample events with surveys dataset
events_with_surveys_df = clr.sample_data.events_with_surveys
events_with_surveys_df.to_csv('input/events_with_surveys.csv', index=False)
print(f"Created input/events_with_surveys.csv: {events_with_surveys_df.shape}")
```

**Sample Data Preview:**

**Users Table:**
| user_id | name           | reg_date_time        | zipcode |
|---------|----------------|---------------------|---------|
| 101     | Alice Johnson  | 2020-01-15 10:30:00 | 10001   |
| 202     | Bob Smith      | 2019-06-22 14:45:00 | 90210   |
| 303     | Charlie Brown  | 2021-03-08 09:15:00 | 60601   |
| 404     | Diana Prince   | 2018-11-12 16:20:00 | 33101   |
| 505     | Eve Wilson     | 2022-07-03 11:55:00 | 98101   |

**Events with Surveys Table:**
| user_id | event_name            | event_value          | event_date_time        |
|---------|----------------------|---------------------|------------------------|
| 101     | sensor_1             | 100.0               | 2023-01-10 08:30:00    |
| 101     | sensor_2             | 250.0               | 2023-01-15 14:20:00    |
| 202     | Survey submission date| 2023-01-20 10:15:00 | 2023-01-20 10:15:00    |
| 202     | user submitted       | 101                 | 2023-01-20 10:16:00    |
| 202     | sensor_1             | 50.0                | 2023-02-05 09:45:00    |
| 303     | sensor_3             | 0.0                 | 2023-02-05 17:30:00    |
| 303     | sensor_1             | 75.0                | 2023-03-12 10:15:00    |
| 404     | sensor_2             | 300.0               | 2023-03-12 15:45:00    |
| 505     | sensor_1             | 25.0                | 2023-04-08 11:20:00    |
| 505     | sensor_1             | 150.0               | 2023-05-20 13:10:00    |
| 505     | Survey submission date| 2023-02-12 14:30:00 | 2023-02-12 14:30:00    |
| 505     | user submitted       | 202                 | 2023-02-12 14:31:00    |
| 303     | sensor_2             | 400.0               | 2023-05-25 16:30:00    |
| 303     | sensor_3             | 0.0                 | 2023-05-25 18:45:00    |
| 303     | Survey submission date| 2023-03-18 09:45:00 | 2023-03-18 09:45:00    |
| 303     | user submitted       | 303                 | 2023-03-18 09:46:00    |

Note that:
- For "Survey submission date" events, the `event_value` column contains a datetime value representing when the survey was submitted.
- For "user submitted" events, the `event_value` column contains a user_id value (101, 202, or 303) that needs to be de-identified.

## 2. Create Filtered De-identification Configuration

Create a configuration that demonstrates filtered de-identification where:
- "Survey submission date" events have both their `event_date_time` and `event_value` columns de-identified
- "user submitted" events have their `event_value` column (which contains user_id values) de-identified

### 2.1 Create the Main Configuration File

Create `filter_deid_config.yaml`. This configuration uses Hydra's import functionality, which is explained in detail in the [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md#3-understanding-hydras-import-functionality) tutorial:

```yaml
# Filtered de-identification pipeline configuration
# This configuration demonstrates selective de-identification based on event types
# For "Survey submission date" events, both event_date_time and event_value are de-identified

name: "filtered_deid_pipeline"

# De-identification configuration
deid_config:
  time_shift:
    method: "shift_by_days"
    min: 30
    max: 90

# Import configurations from separate files
# See multi-table tutorial section 3 for details on how Hydra imports work
defaults:
  - io_config
  - users_table_config
  - events_with_surveys_table_config

# Tables will be automatically merged from the imported configs
```

### 2.2 Create IO Configuration File

Create `io_config.yaml` for input/output settings. The IO configuration is identical to the one used in the [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md#22-create-io-configuration-file) tutorial. See that section for the complete code.

### 2.3 Create Table Configuration Files

Create separate files for each table's configuration:

**`users_table_config.yaml`:**

The users table configuration is identical to the one in the [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md#23-create-table-configuration-files) tutorial. See that section for the complete code. The only difference is the time shift method (`shift_by_days` instead of `shift_by_years`), but the structure is the same.

**`events_with_surveys_table_config.yaml`:**
```yaml
# Events with surveys table configuration
# This table demonstrates filtered de-identification where:
# - "Survey submission date" events have both event_date_time and event_value (datetime) de-identified
# - "user submitted" events have event_value (user_id) de-identified

tables:
  events_with_surveys:
    name: "events_with_surveys"
    depends_on: ["users"]
    transformers:
      - method: "IDDeidentifier"
        # ... see similar sections from [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md#23-create-table-configuration-files)
      - method: "DateTimeDeidentifier" # de-identify the column event_date_time for the entire table
        # ... see similar sections from [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md#23-create-table-configuration-files)
      - method: "DateTimeDeidentifier" # de-identify the value column only for filtered rows
        uid: "events_value_datetime_transformer"
        depends_on: ["events_datetime_transformer"]
        configs:
          idconfig:
            name: "user_id"
            uid: "user_uid"
            description: "User identifier"
          deid_config:
            time_shift:
              method: "shift_by_days"
              min: 30
              max: 90
          datetime_column: "event_value"
        filter:
          where_condition: "event_name == 'Survey submission date'"
        value_cast: "datetime"  # Cast string datetime values to datetime type
      - method: "IDDeidentifier"
        uid: "events_value_id_transformer"
        depends_on: ["events_value_datetime_transformer"]
        configs:
          idconfig:
            name: "event_value"
            uid: "user_uid"
            description: "User ID in event_value for user submitted events (uses same mapping as user_id)"
        filter:
          where_condition: "event_name == 'user submitted'"
        value_cast: "integer"  # Cast string ID values to integer to match user_id type
```

## 3. Understanding Filtered De-identification

### 3.1 How Filtered De-identification Works

The key difference in this configuration is the `filter` section in the transformers:

```yaml
filter:
  where_condition: "event_name == 'Survey submission date'"
```

or

```yaml
filter:
  where_condition: "event_name == 'user submitted'"
```

This means:
- **Only rows where `event_name == 'Survey submission date'`** will have their `event_date_time` and `event_value` (datetime) de-identified
- **Only rows where `event_name == 'user submitted'`** will have their `event_value` (user_id) de-identified
- **All other rows** (sensor_1, sensor_2, sensor_3 events) will keep their original timestamps and values
- **All rows** will still have their `user_id` column de-identified (no filter on the main ID transformer)

### 3.2 Multiple Column and Event Type De-identification

In this example, we de-identify **multiple columns** for **different event types**:

**For "Survey submission date" events:**
1. **`event_date_time`**: The timestamp when the event occurred
2. **`event_value`**: The survey submission date stored in the value column (datetime)
   - Uses `value_cast: "datetime"` to convert string datetime values to datetime type before de-identification

**For "user submitted" events:**
1. **`event_value`**: The user_id value stored in the value column (integer ID)
   - Uses `value_cast: "integer"` to convert string ID values to integer type to match the user_id type and ensure consistent de-identification mappings

This demonstrates how you can apply different de-identification strategies to different event types using filters, and how `value_cast` ensures proper type handling when data is read from CSV files (where numeric and datetime values are often stored as strings).

For more information about the `value_cast` option, see the [Value Casting Guide](value-casting.md).

### 3.3 Filter Configuration

The `filter` configuration uses SQL-like WHERE conditions:

```yaml
filter:
  where_condition: "event_name == 'Survey submission date'"
```

**Supported filter conditions:**
- `event_name == 'Survey submission date'` - Exact string match
- `event_name in ['Survey submission date', 'Other event']` - Multiple values
- `event_value > 100` - Numeric comparisons
- `event_name == 'Survey submission date' and user_id == 101` - Complex conditions

### 3.4 Processing Flow

1. **Users table**: All data is de-identified (IDs, timestamps, names dropped)
2. **Events table**: 
   - All `user_id` values are de-identified using the same mapping as users
   - Only "Survey submission date" events have their `event_date_time` de-identified
   - Only "Survey submission date" events have their `event_value` (datetime) de-identified
   - Only "user submitted" events have their `event_value` (user_id) de-identified
   - Other events (`sensor_1`, `sensor_2`, `sensor_3`) keep their original timestamps and values

## 4. Verify the Configuration

Before running the de-identification, verify that the configuration is loaded correctly. The verification process is similar to the one described in the [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md#4-verify-the-merged-configuration) tutorial, with additional checks for the filter configurations:

```python
import cleared as clr
from cleared.cli.utils import load_config_from_file

# Load the merged configuration
config = load_config_from_file("filter_deid_config.yaml")

print("Configuration loaded successfully!")
print(f"Pipeline name: {config.name}")
print(f"Tables loaded: {list(config.tables.keys())}")

# Check each table's transformers (see multi-table tutorial for base structure)
for table_name, table_config in config.tables.items():
    print(f"\n{table_name} table:")
    print(f"  - Dependencies: {table_config.depends_on}")
    print(f"  - Transformers: {len(table_config.transformers)}")
    for i, transformer in enumerate(table_config.transformers):
        print(f"    {i+1}. {transformer.method} ({transformer.uid})")
        # Check for filter configuration (unique to this tutorial)
        if transformer.filter is not None:
            print(f"       Filter: {transformer.filter.where_condition}")

# Additional checks specific to filtered de-identification
events_table = config.tables["events_with_surveys"]
datetime_transformer = next(t for t in events_table.transformers if t.uid == "events_datetime_transformer")
value_datetime_transformer = next(t for t in events_table.transformers if t.uid == "events_value_datetime_transformer")
value_id_transformer = next(t for t in events_table.transformers if t.uid == "events_value_id_transformer")

print(f"\nFiltered transformer configurations:")
print(f"  Events datetime transformer:")
print(f"    - Filter condition: {datetime_transformer.filter.where_condition}")
print(f"    - Datetime column: {datetime_transformer.configs['datetime_column']}")
print(f"  Events value datetime transformer:")
print(f"    - Filter condition: {value_datetime_transformer.filter.where_condition}")
print(f"    - Datetime column: {value_datetime_transformer.configs['datetime_column']}")
print(f"    - Value cast: {value_datetime_transformer.value_cast}")
print(f"  Events value ID transformer:")
print(f"    - Filter condition: {value_id_transformer.filter.where_condition}")
print(f"    - ID column: {value_id_transformer.configs['idconfig']['name']}")
print(f"    - Value cast: {value_id_transformer.value_cast}")
```

Expected output:
```
Configuration loaded successfully!
Pipeline name: filtered_deid_pipeline
Tables loaded: ['users', 'events_with_surveys']

users table:
  - Dependencies: []
  - Transformers: 3
    1. IDDeidentifier (user_id_transformer)
    2. DateTimeDeidentifier (users_datetime_transformer)
    3. ColumnDropper (name_drop_transformer)

events_with_surveys table:
  - Dependencies: ['users']
  - Transformers: 4
    1. IDDeidentifier (events_user_id_transformer)
    2. DateTimeDeidentifier (events_datetime_transformer)
       Filter: event_name == 'Survey submission date'
    3. DateTimeDeidentifier (events_value_datetime_transformer)
       Filter: event_name == 'Survey submission date'
       Value cast: datetime
    4. IDDeidentifier (events_value_id_transformer)
       Filter: event_name == 'user submitted'
       Value cast: integer

Filtered transformer configurations:
  Events datetime transformer:
    - Filter condition: event_name == 'Survey submission date'
    - Datetime column: event_date_time
  Events value datetime transformer:
    - Filter condition: event_name == 'Survey submission date'
    - Datetime column: event_value
  Events value ID transformer:
    - Filter condition: event_name == 'user submitted'
    - ID column: event_value
```

## 5. Run De-identification with CLI

Use the Cleared CLI to run the filtered de-identification:

```bash
cleared run filter_deid_config.yaml
```

Expected output:
```
Loaded configuration: filtered_deid_pipeline
Processing table: users
  - Running transformer: user_id_transformer (IDDeidentifier)
  - Running transformer: users_datetime_transformer (DateTimeDeidentifier)
  - Running transformer: name_drop_transformer (ColumnDropper)
Processing table: events_with_surveys
  - Running transformer: events_user_id_transformer (IDDeidentifier)
  - Running transformer: events_datetime_transformer (DateTimeDeidentifier)
  - Running transformer: events_value_datetime_transformer (DateTimeDeidentifier)
  - Running transformer: events_value_id_transformer (IDDeidentifier)
De-identification complete!

Results:
  users: (5, 4) -> (5, 3)
  events_with_surveys: (16, 4) -> (16, 4)
```

## 6. Run De-identification with Engine

Alternatively, use the Cleared Engine programmatically. The code is identical to the one shown in the [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md#6-run-de-identification-with-engine) tutorial. See that section for the complete code example.

## 7. Verify Results

Check the output files to see the filtered de-identification in action. The verification process is similar to the one described in the [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md#7-verify-results) tutorial:

```bash
# Check the de-identified data
cat output/users.csv
cat output/events_with_surveys.csv

# Check the de-identification reference tables
ls deid_ref/
cat deid_ref/user_uid.csv
cat deid_ref/user_uid_shift.csv
```

**Expected Results:**

**Users Table (all data de-identified):**
```csv
user_id,reg_date_time,zipcode
2,2020-02-14 10:30:00,10001
4,2019-07-22 14:45:00,90210
5,2021-04-07 09:15:00,60601
1,2018-12-12 16:20:00,33101
3,2022-08-02 11:55:00,98101
```

**Events with Surveys Table (selective de-identification):**
```csv
user_id,event_name,event_value,event_date_time
2,sensor_1,100.0,2023-01-10 08:30:00
2,sensor_2,250.0,2023-01-15 14:20:00
4,Survey submission date,2023-02-19 10:15:00,2023-02-19 10:15:00
4,user submitted,2,2023-01-20 10:16:00
4,sensor_1,50.0,2023-02-05 09:45:00
5,sensor_3,0.0,2023-02-05 17:30:00
5,sensor_1,75.0,2023-03-12 10:15:00
1,sensor_2,300.0,2023-03-12 15:45:00
1,sensor_1,25.0,2023-04-08 11:20:00
3,sensor_1,150.0,2023-05-20 13:10:00
3,Survey submission date,2023-03-14 14:30:00,2023-03-14 14:30:00
3,user submitted,4,2023-02-12 14:31:00
3,sensor_2,400.0,2023-05-25 16:30:00
3,sensor_3,0.0,2023-05-25 18:45:00
5,Survey submission date,2023-04-17 09:45:00,2023-04-17 09:45:00
5,user submitted,5,2023-03-18 09:46:00
```

Note: 
- The survey submission dates shown above are examples of de-identified dates (shifted by 30-90 days from the original dates). The actual shifted dates will vary based on the random time shift applied.
- The "user submitted" events have their `event_value` de-identified (original values 101, 202, 303 become 2, 4, 5 to match the de-identified user_ids).
- The sensor events (sensor_1, sensor_2, sensor_3) keep their original timestamps and values unchanged.

**Key Observations:**
- ✅ **user_id**: Consistently de-identified across all tables (1, 2, 3, 4, 5)
- ✅ **Survey submission date events**: Both `event_date_time` and `event_value` (datetime) are de-identified (shifted by 30-90 days)
- ✅ **user submitted events**: The `event_value` (user_id) is de-identified to match the de-identified user_ids (101→2, 202→4, 303→5)
- ✅ **Other events**: Both timestamps and values remain unchanged (sensor_1, sensor_2, sensor_3)
- ✅ **Referential integrity**: Maintained across all tables
- ✅ **Multiple filter types**: Different event types use different de-identification strategies (datetime shift vs ID de-identification)

## 8. Advanced Filtering Examples

### 8.1 Multiple Event Types

To de-identify multiple event types:

```yaml
filter:
  where_condition: "event_name in ['Survey submission date', 'Medical exam date', 'Appointment date']"
```

### 8.2 Complex Conditions

To de-identify based on multiple criteria:

```yaml
filter:
  where_condition: "event_name == 'Survey submission date' and event_value >= '2023-01-01'"
```

### 8.3 Numeric Filters

To de-identify based on numeric values:

```yaml
filter:
  where_condition: "event_value > 100"
```

### 8.4 Date Range Filters

To de-identify events within a specific date range:

```yaml
filter:
  where_condition: "event_date_time >= '2023-01-01' and event_date_time < '2023-02-01'"
```

## 9. Use Cases for Filtered De-identification

### 9.1 Healthcare Scenarios

- **Sensitive Events**: De-identify only sensitive medical events while preserving routine checkups
- **Time-Sensitive Data**: De-identify only critical timestamps while keeping relative timing
- **Conditional Privacy**: Apply different de-identification rules based on patient consent

### 9.2 Research Scenarios

- **Survey Data**: De-identify survey submission dates while preserving other event timestamps
- **Cohort Studies**: De-identify only specific event types for privacy while preserving research utility
- **Longitudinal Data**: Apply selective de-identification to maintain temporal relationships

### 9.3 E-commerce Scenarios

- **Delivery Events**: De-identify only delivery and shipping timestamps
- **Payment Events**: De-identify only payment-related timestamps
- **User Activity**: De-identify only sensitive user activities

## 10. Troubleshooting

### Common Issues

For general troubleshooting related to multi-table configurations, see the [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md#8-troubleshooting) tutorial. Additional issues specific to filtered de-identification:

1. **Filter condition syntax**: Ensure SQL-like syntax is correct
2. **Column names**: Verify column names in filter conditions match the data
3. **Data types**: Ensure filter conditions match the data types in your data
4. **Empty results**: Check if filter conditions are too restrictive

### Debug Mode

Add debug output to see which rows are being filtered:

```python
# Check which rows match the filter condition
import pandas as pd

events_df = pd.read_csv("input/events_with_surveys.csv")
filtered_rows = events_df.query("event_name == 'Survey submission date'")
print(f"Rows matching filter: {len(filtered_rows)}")
print(filtered_rows[['event_name', 'event_date_time', 'event_value']])
```

## Next Steps

- Learn about [value casting](value-casting.md) for handling type conversions in transformers
- Learn about [multi-table pipeline configuration](multi_table_pipeline_config.md) for complex scenarios
- Check out [custom transformers](custom-transformers-plugins.md) for specialized de-identification needs
- Explore [performance optimization](performance_tips.md) for large datasets with filtering