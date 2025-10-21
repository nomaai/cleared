# Rerun De-identification with Consistency

This tutorial demonstrates how to rerun de-identification on updated data while maintaining consistency with previously de-identified records. It shows how to use versioned de-identification reference tables to ensure that existing IDs remain the same while new IDs are properly de-identified.

## Prerequisites

Make sure you have Cleared installed:

```bash
pip install cleared
```

## Setup Working Directory

Create a working directory with the required structure:

```bash
mkdir -p ~/cleared_rerun/{input,output,deid_ref_tables/v1,deid_ref_tables/v2}
cd ~/cleared_rerun
```

**Important**: The `deid_ref_tables/v1` directory must exist before running the first de-identification, even though it will be empty initially.

## 1. Initial De-identification Setup

First, let's set up the initial de-identification using the same configuration approach as the [single-table tutorial](use_cleared_config.md).

### 1.1 Create Initial Sample Data

```python
import cleared as clr

# Get initial sample users data
users_df = clr.sample_data.users_single_table
users_df.to_csv('input/users.csv', index=False)
print("Created initial input/users.csv")
```

**Initial Data Preview:**
| user_id | name           | reg_date_time        | zipcode |
|---------|----------------|---------------------|---------|
| 101     | Alice Johnson  | 2020-01-15 10:30:00 | 10001   |
| 202     | Bob Smith      | 2019-06-22 14:45:00 | 90210   |
| 303     | Charlie Brown  | 2021-03-08 09:15:00 | 60601   |
| 404     | Diana Prince   | 2018-11-12 16:20:00 | 33101   |
| 505     | Eve Wilson     | 2022-07-03 11:55:00 | 98101   |

### 1.2 Create Versioned Configuration

Create `users_config_v1.yaml` with versioned de-identification reference paths:

```yaml
name: "users_deid_pipeline_v1"
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
        base_path: "./deid_ref_tables/v1"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./deid_ref_tables/v1"
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
          deid_config:
            time_shift:
              method: "shift_by_years"
              min: -5
              max: 5
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

### 1.3 Run Initial De-identification

```bash
cleared run users_config_v1.yaml
```

Expected output:
```
Loaded configuration: users_deid_pipeline_v1
Processing table: users
  - Running transformer: user_id_transformer (IDDeidentifier)
  - Running transformer: datetime_transformer (DateTimeDeidentifier)
  - Running transformer: name_drop_transformer (ColumnDropper)
De-identification complete!

Results:
  users: (5, 4) -> (5, 3)
  Columns: ['user_id', 'reg_date_time', 'zipcode']
```

### 1.4 Verify Initial Results

```python
import pandas as pd

# Check the de-identified data
deid_users = pd.read_csv('output/users.csv')
print("Initial de-identified data:")
print(deid_users)

# Check the de-identification reference tables
import os
print(f"\nDe-identification reference files in v1:")
for file in os.listdir('deid_ref_tables/v1'):
    print(f"  - {file}")
```

Expected output:
```
Initial de-identified data:
   user_id  reg_date_time  zipcode
0        1     2017-01-15    10001
1        2     2020-06-22    90210
2        3     2021-03-08    60601
3        4     2018-11-12    33101
4        5     2020-07-03    98101

De-identification reference files in v1:
  - user_uid.csv
  - user_uid_shift.csv
```

## 2. Add New Data and Rerun De-identification

Now let's add 5 new users and rerun the de-identification while maintaining consistency.

### 2.1 Add New Users

```python
import pandas as pd
from datetime import datetime

# Create 5 new users
new_users_df = pd.DataFrame({
    'user_id': [606, 707, 808, 909, 1010],
    'name': ['Frank Miller', 'Grace Lee', 'Henry Davis', 'Ivy Chen', 'Jack Wilson'],
    'reg_date_time': [
        datetime(2023, 2, 14, 8, 30),
        datetime(2023, 4, 10, 12, 15),
        datetime(2023, 6, 5, 16, 45),
        datetime(2023, 8, 20, 9, 20),
        datetime(2023, 10, 12, 14, 10)
    ],
    'zipcode': ['20001', '30002', '40003', '50004', '60005']
})

# Append to existing data
existing_users = pd.read_csv('input/users.csv')
combined_users = pd.concat([existing_users, new_users_df], ignore_index=True)
combined_users.to_csv('input/users.csv', index=False)

print("Added 5 new users to input/users.csv")
print(f"Total users: {len(combined_users)}")
print("\nNew users:")
print(new_users_df)
```

**New Data Preview:**
| user_id | name        | reg_date_time        | zipcode |
|---------|-------------|---------------------|---------|
| 606     | Frank Miller| 2023-02-14 08:30:00 | 20001   |
| 707     | Grace Lee   | 2023-04-10 12:15:00 | 30002   |
| 808     | Henry Davis | 2023-06-05 16:45:00 | 40003   |
| 909     | Ivy Chen    | 2023-08-20 09:20:00 | 50004   |
| 1010    | Jack Wilson | 2023-10-12 14:10:00 | 60005   |

### 2.2 Create Version 2 Configuration

Create `users_config_v2.yaml` that reads from v1 and writes to v2:

```yaml
name: "users_deid_pipeline_v2"
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
        base_path: "./deid_ref_tables/v1"  # Read from v1
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./deid_ref_tables/v2"  # Write to v2
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
          deid_config:
            time_shift:
              method: "shift_by_years"
              min: -5
              max: 5
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

### 2.3 Run Rerun De-identification

```bash
cleared run users_config_v2.yaml
```

Expected output:
```
Loaded configuration: users_deid_pipeline_v2
Processing table: users
  - Running transformer: user_id_transformer (IDDeidentifier)
  - Running transformer: datetime_transformer (DateTimeDeidentifier)
  - Running transformer: name_drop_transformer (ColumnDropper)
De-identification complete!

Results:
  users: (10, 4) -> (10, 3)
  Columns: ['user_id', 'reg_date_time', 'zipcode']
```

## 3. Verify Consistency

Let's verify that the de-identification maintained consistency for existing users while properly handling new users.

### 3.1 Compare De-identified Results

```python
import pandas as pd

# Load both versions of de-identified data
v1_deid = pd.read_csv('output/users.csv')  # This will be overwritten, so we need to save it first
# Let's recreate the v1 results for comparison
print("=== CONSISTENCY VERIFICATION ===")

# Load the current de-identified data (v2)
v2_deid = pd.read_csv('output/users.csv')
print(f"\nTotal users in v2: {len(v2_deid)}")
print("v2 De-identified data:")
print(v2_deid)

# Check the de-identification reference tables
print(f"\nDe-identification reference files in v1:")
for file in os.listdir('deid_ref_tables/v1'):
    print(f"  - {file}")

print(f"\nDe-identification reference files in v2:")
for file in os.listdir('deid_ref_tables/v2'):
    print(f"  - {file}")

# Load and compare reference tables
v1_user_ref = pd.read_csv('deid_ref_tables/v1/user_uid.csv')
v2_user_ref = pd.read_csv('deid_ref_tables/v2/user_uid.csv')

print(f"\nUser ID mappings in v1: {len(v1_user_ref)} mappings")
print("v1 mappings:")
print(v1_user_ref)

print(f"\nUser ID mappings in v2: {len(v2_user_ref)} mappings")
print("v2 mappings:")
print(v2_user_ref)
```

### 3.2 Verify Consistency Programmatically

```python
# Check that existing users have consistent de-identified IDs
print("\n=== CONSISTENCY CHECK ===")

# Original user IDs from v1
original_v1_users = [101, 202, 303, 404, 505]

# Check if these users have the same de-identified IDs in v2
v1_mappings = dict(zip(v1_user_ref['user_id'], v1_user_ref['user_uid']))
v2_mappings = dict(zip(v2_user_ref['user_id'], v2_user_ref['user_uid']))

print("Consistency check for original users:")
for orig_id in original_v1_users:
    v1_deid_id = v1_mappings.get(orig_id)
    v2_deid_id = v2_mappings.get(orig_id)
    
    if v1_deid_id == v2_deid_id:
        print(f"  ✅ User {orig_id}: {v1_deid_id} (consistent)")
    else:
        print(f"  ❌ User {orig_id}: {v1_deid_id} -> {v2_deid_id} (inconsistent)")

# Check that new users got new de-identified IDs
new_user_ids = [606, 707, 808, 909, 1010]
print(f"\nNew users de-identified IDs:")
for new_id in new_user_ids:
    deid_id = v2_mappings.get(new_id)
    print(f"  User {new_id}: {deid_id}")

print(f"\nTotal mappings: {len(v2_mappings)} (5 original + 5 new)")
```

Expected output:
```
=== CONSISTENCY CHECK ===
Consistency check for original users:
  ✅ User 101: 1 (consistent)
  ✅ User 202: 2 (consistent)
  ✅ User 303: 3 (consistent)
  ✅ User 404: 4 (consistent)
  ✅ User 505: 5 (consistent)

New users de-identified IDs:
  User 606: 6
  User 707: 7
  User 808: 8
  User 909: 9
  User 1010: 10

Total mappings: 10 (5 original + 5 new)
```

## 4. Understanding the Versioned Approach

### 4.1 How Versioning Works

The versioned approach ensures consistency by:

1. **Reading existing mappings**: The v2 configuration reads from `deid_ref_tables/v1` to get existing ID mappings
2. **Preserving existing mappings**: Existing user IDs keep their same de-identified values
3. **Adding new mappings**: New user IDs get new sequential de-identified values
4. **Writing updated mappings**: All mappings (old + new) are written to `deid_ref_tables/v2`

### 4.2 Configuration Key Changes

The key differences between v1 and v2 configurations:

```yaml
# v1 Configuration
deid_ref:
  input_config:
    base_path: "./deid_ref_tables/v1"   # No existing data
  output_config:
    base_path: "./deid_ref_tables/v1"   # Write initial mappings

# v2 Configuration  
deid_ref:
  input_config:
    base_path: "./deid_ref_tables/v1"   # Read existing mappings
  output_config:
    base_path: "./deid_ref_tables/v2"   # Write updated mappings
```

### 4.3 Benefits of Versioning

1. **Consistency**: Existing records maintain their de-identified IDs
2. **Traceability**: Each version has its own reference tables
3. **Rollback**: Can revert to previous versions if needed
4. **Audit Trail**: Clear history of de-identification changes
5. **Incremental Processing**: Only process new data while preserving existing mappings

## 5. Advanced Scenarios

### 5.1 Handling Multiple Reruns

For subsequent reruns, you can create v3, v4, etc.:

```yaml
# v3 Configuration
deid_ref:
  input_config:
    base_path: "./deid_ref_tables/v2"   # Read from previous version
  output_config:
    base_path: "./deid_ref_tables/v3"   # Write to new version
```

### 5.2 Selective Rerun

You can also rerun with only specific tables or transformers by modifying the configuration.

### 5.3 Data Validation

Always validate consistency after reruns:

```python
def validate_consistency(prev_version, curr_version):
    """Validate that existing IDs remain consistent."""
    prev_ref = pd.read_csv(f'deid_ref_tables/{prev_version}/user_uid.csv')
    curr_ref = pd.read_csv(f'deid_ref_tables/{curr_version}/user_uid.csv')
    
    prev_mappings = dict(zip(prev_ref['user_id'], prev_ref['user_uid']))
    curr_mappings = dict(zip(curr_ref['user_id'], curr_ref['user_uid']))
    
    for orig_id, prev_deid in prev_mappings.items():
        curr_deid = curr_mappings.get(orig_id)
        if prev_deid != curr_deid:
            return False, f"Inconsistent mapping for user {orig_id}"
    
    return True, "All mappings consistent"
```

## Next Steps

- Learn about [multi-table consistency](multi_table_pipeline_config.md) for related data
- Check out [performance optimization](performance_tips.md) for large datasets
- Explore [custom transformers](custom_transformers.md) for specialized de-identification needs
