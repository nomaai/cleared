# Multi-Table Pipeline Configuration

This guide shows you how to set up and run multi-table de-identification pipelines using Cleared with configuration files. This extends the [single-table configuration approach](use_cleared_config.md) to handle related tables that need consistent de-identification across foreign key relationships.

## Prerequisites

Make sure you have Cleared installed:

```bash
pip install cleared
```

## Setup Working Directory

Create a working directory with the required structure:

```bash
mkdir -p ~/cleared_multi_table/{input,output,deid_ref}
cd ~/cleared_multi_table
```

## 1. Create Sample Data

Create CSV files for three related tables using Cleared's sample data:

```python
import cleared as clr

# Get sample datasets and save to input directory
datasets = clr.sample_data.multi_table_datasets
for table_name, df in datasets.items():
    df.to_csv(f'input/{table_name}.csv', index=False)
    print(f"Created input/{table_name}.csv: {df.shape}")
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

**Events Table:**
| user_id | event_name | event_value | event_date_time        |
|---------|------------|-------------|------------------------|
| 101     | sensor_1   | 100.0       | 2023-01-10 08:30:00    |
| 101     | sensor_2   | 250.0       | 2023-01-15 14:20:00    |
| 202     | sensor_1   | 50.0        | 2023-02-05 09:45:00    |
| 202     | sensor_3   | 0.0         | 2023-02-05 17:30:00    |
| 303     | sensor_1   | 75.0        | 2023-03-12 10:15:00    |
| 303     | sensor_2   | 300.0       | 2023-03-12 15:45:00    |
| 404     | sensor_1   | 25.0        | 2023-04-08 11:20:00    |
| 505     | sensor_1   | 150.0       | 2023-05-20 13:10:00    |
| 505     | sensor_2   | 400.0       | 2023-05-25 16:30:00    |
| 505     | sensor_3   | 0.0         | 2023-05-25 18:45:00    |

**Orders Table:**
| user_id | order_id | order_name | order_date_time        |
|---------|----------|------------|------------------------|
| 101     | 1001     | Laptop     | 2023-01-20 10:15:00    |
| 202     | 1002     | Mouse      | 2023-02-10 14:30:00    |
| 303     | 1003     | Keyboard   | 2023-03-15 09:45:00    |
| 404     | 1004     | Monitor    | 2023-04-12 16:20:00    |
| 505     | 1005     | Headphones | 2023-05-30 11:55:00    |
| 101     | 1006     | Charger    | 2023-06-05 13:25:00    |
| 202     | 1007     | Desk       | 2023-06-15 15:40:00    |
| 303     | 1008     | Chair      | 2023-07-02 12:10:00    |

## 2. Create Modular Multi-Table Configuration

We'll use Hydra's import functionality to organize our configuration into separate, reusable files. This approach makes the configuration more maintainable and allows for better organization.

### 2.1 Create the Main Configuration File

Create `multi_table_config.yaml` that imports other configuration files:

```yaml
# Multi-table de-identification pipeline configuration
# This configuration uses Hydra's import functionality to organize
# configuration into separate, reusable files

name: "multi_table_deid_pipeline"

# De-identification configuration
deid_config:
  time_shift:
    method: "shift_by_years"
    min: -5
    max: 5

# Import configurations from separate files
defaults:
  - io_config
  - users_table_config
  - events_table_config  
  - orders_table_config

# Tables will be automatically merged from the imported configs
```

### 2.2 Create IO Configuration File

Create `io_config.yaml` for input/output settings:

```yaml
# IO Configuration for Cleared pipelines
# This file contains the input/output configuration for data and de-identification reference tables

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

### 2.3 Create Table Configuration Files

Create separate files for each table's configuration:

**`users_table_config.yaml`:**
```yaml
# Users table configuration
# This table has no dependencies and is processed first

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
        uid: "users_datetime_transformer"
        depends_on: ["user_id_transformer"]
        configs:
          idconfig:
            name: "user_id"
            uid: "user_uid"
            description: "User identifier"
          datetime_column: "reg_date_time"
      - method: "ColumnDropper"
        uid: "name_drop_transformer"
        depends_on: ["users_datetime_transformer"]
        configs:
          idconfig:
            name: "name"
            uid: "name_drop"
            description: "User name to drop"
```

**`events_table_config.yaml`:**
```yaml
# Events table configuration
# This table depends on users table and uses the same user_id de-identification

tables:
  events:
    name: "events"
    depends_on: ["users"]
    transformers:
      - method: "IDDeidentifier"
        uid: "events_user_id_transformer"
        depends_on: []
        configs:
          idconfig:
            name: "user_id"
            uid: "user_uid"
            description: "User identifier"
      - method: "DateTimeDeidentifier"
        uid: "events_datetime_transformer"
        depends_on: ["events_user_id_transformer"]
        configs:
          idconfig:
            name: "user_id"
            uid: "user_uid"
            description: "User identifier"
          datetime_column: "event_date_time"
```

**`orders_table_config.yaml`:**
```yaml
# Orders table configuration
# This table depends on events table and uses the same user_id de-identification

tables:
  orders:
    name: "orders"
    depends_on: ["events"]
    transformers:
      - method: "IDDeidentifier"
        uid: "orders_user_id_transformer"
        depends_on: []
        configs:
          idconfig:
            name: "user_id"
            uid: "user_uid"
            description: "User identifier"
      - method: "DateTimeDeidentifier"
        uid: "orders_datetime_transformer"
        depends_on: ["orders_user_id_transformer"]
        configs:
          idconfig:
            name: "user_id"
            uid: "user_uid"
            description: "User identifier"
          datetime_column: "order_date_time"
```

## 3. Understanding Hydra's Import Functionality

### 3.1 How Hydra-Style Imports Work

The `defaults` section allows you to import and merge configurations from separate files:

```yaml
defaults:
  - io_config                    # Imports io_config.yaml
  - users_table_config          # Imports users_table_config.yaml
  - events_table_config         # Imports events_table_config.yaml
  - orders_table_config         # Imports orders_table_config.yaml
```

**Key Benefits:**
- **Modularity**: Each table's configuration is in its own file
- **Reusability**: IO config can be shared across different pipelines
- **Maintainability**: Easier to update individual table configurations
- **Organization**: Clear separation of concerns

### 3.2 Configuration Merging

When Cleared loads the main configuration, it automatically:
1. **Imports** each file listed in `defaults`
2. **Merges** the configurations into a single structure using deep merging
3. **Resolves** any conflicts (later imports override earlier ones)

**Key Point**: The individual table configuration files define their tables under a `tables:` key, but when merged, all these tables are automatically placed under a single `tables` key in the final configuration.

The final merged configuration will have:
```yaml
name: "multi_table_deid_pipeline"
deid_config: { ... }
io: { ... }                    # From io_config.yaml
tables:                        # ← All tables merged here automatically
  users: { ... }               # From users_table_config.yaml
  events: { ... }              # From events_table_config.yaml
  orders: { ... }              # From orders_table_config.yaml
```

**How the Merging Works:**
- The main config provides the base structure (`name`, `deid_config`)
- Each imported file adds its specific configuration
- **Tables are automatically merged**: All `tables` sections from imported files are combined into a single `tables` dictionary in the final configuration
- IO configuration is merged into the `io` section
- Deep merging ensures nested structures are properly combined

**Example of the merging process:**
```yaml
# Main config (multi_table_config.yaml)
name: "multi_table_deid_pipeline"
deid_config: { ... }
defaults: [io_config, users_table_config, events_table_config, orders_table_config]

# After merging, the final config becomes:
name: "multi_table_deid_pipeline"
deid_config: { ... }
io: { ... }                    # From io_config.yaml
tables:                        # All tables merged here
  users: { ... }               # From users_table_config.yaml
  events: { ... }              # From events_table_config.yaml
  orders: { ... }              # From orders_table_config.yaml
```

### 3.3 Table Dependencies

The dependency structure remains the same across all files:

```yaml
# In users_table_config.yaml
users:
  depends_on: []  # No dependencies - processed first

# In events_table_config.yaml  
events:
  depends_on: ["users"]  # Depends on users - processed second

# In orders_table_config.yaml
orders:
  depends_on: ["events"]  # Depends on events - processed third
```

This ensures:
1. **Users** table is processed first, creating the initial ID mappings
2. **Events** table uses the same ID mappings from users
3. **Orders** table uses the same ID mappings from events

### 3.4 Consistent De-identification

All tables use the same `user_uid` identifier, ensuring consistent de-identification:

```yaml
configs:
  idconfig:
    name: "user_id"
    uid: "user_uid"  # Same UID across all tables
    description: "User identifier"
```

This means:
- User ID `101` in the users table becomes `1` in all tables
- User ID `202` in the users table becomes `2` in all tables
- And so on...

### 3.5 IO Configuration

The IO configuration is shared across all tables:
- **`data.input_config`**: Reads CSV files from the `./input` directory
- **`data.output_config`**: Writes results to the `./output` directory  
- **`deid_ref`**: Stores de-identification reference tables in `./deid_ref` directory
- **`runtime_io_path`**: Temporary files during processing

### 3.6 Benefits of Modular Configuration

The modular approach provides several advantages:

**1. Reusability**
- IO configuration can be shared across different pipelines
- Table configurations can be reused in different combinations

**2. Maintainability**
- Each table's configuration is isolated and easy to update
- Changes to one table don't affect others
- Clear separation of concerns

**3. Scalability**
- Easy to add new tables by creating new config files
- Simple to remove tables by removing imports
- Supports different pipeline variants

**4. Team Collaboration**
- Different team members can work on different table configs
- Version control shows changes per table
- Easier code reviews

**5. Testing**
- Can test individual table configurations in isolation
- Easier to create test-specific configurations
- Better debugging capabilities

### 3.7 Adding New Tables

To add a new table to your pipeline:

1. **Create a new table config file** (e.g., `products_table_config.yaml`):
```yaml
# Products table configuration
tables:
  products:
    name: "products"
    depends_on: ["orders"]  # Depends on orders table
    transformers:
      - method: "IDDeidentifier"
        uid: "products_id_transformer"
        depends_on: []
        configs:
          idconfig:
            name: "product_id"
            uid: "product_uid"
            description: "Product identifier"
```

2. **Add the import to your main config**:
```yaml
defaults:
  - io_config
  - users_table_config
  - events_table_config  
  - orders_table_config
  - products_table_config  # Add this line
```

3. **Update dependencies** in other tables if needed:
```yaml
# In orders_table_config.yaml
orders:
  depends_on: ["events"]  # Keep existing dependencies
  # ... transformers ...

# In products_table_config.yaml  
products:
  depends_on: ["orders"]  # New dependency
  # ... transformers ...
```

## 4. Verify the Merged Configuration

Before running the de-identification, you can verify that the configuration merging worked correctly:

```python
import cleared as clr
from cleared.cli.utils import load_config_from_file

# Load the merged configuration
config = load_config_from_file("multi_table_config.yaml")

print("Configuration loaded successfully!")
print(f"Pipeline name: {config.name}")
print(f"Tables loaded: {list(config.tables.keys())}")

# Check each table's transformers
for table_name, table_config in config.tables.items():
    print(f"\n{table_name} table:")
    print(f"  - Dependencies: {table_config.depends_on}")
    print(f"  - Transformers: {len(table_config.transformers)}")
    for i, transformer in enumerate(table_config.transformers):
        print(f"    {i+1}. {transformer.method} ({transformer.uid})")
```

Expected output:
```
Configuration loaded successfully!
Pipeline name: multi_table_deid_pipeline
Tables loaded: ['users', 'events', 'orders']

users table:
  - Dependencies: []
  - Transformers: 3
    1. IDDeidentifier (user_id_transformer)
    2. DateTimeDeidentifier (users_datetime_transformer)
    3. ColumnDropper (name_drop_transformer)

events table:
  - Dependencies: ['users']
  - Transformers: 2
    1. IDDeidentifier (events_user_id_transformer)
    2. DateTimeDeidentifier (events_datetime_transformer)

orders table:
  - Dependencies: ['events']
  - Transformers: 2
    1. IDDeidentifier (orders_user_id_transformer)
    2. DateTimeDeidentifier (orders_datetime_transformer)
```

This confirms that:
- ✅ All 3 tables are properly loaded under the `tables` key
- ✅ Each table has the correct dependencies
- ✅ Each table has the expected transformers
- ✅ The configuration merging worked correctly

## 5. Run De-identification with CLI

Use the Cleared CLI to run the multi-table de-identification:

```bash
cleared run multi_table_config.yaml
```

Expected output:
```
Loaded configuration: multi_table_deid_pipeline
Processing table: users
  - Running transformer: user_id_transformer (IDDeidentifier)
  - Running transformer: users_datetime_transformer (DateTimeDeidentifier)
  - Running transformer: name_drop_transformer (ColumnDropper)
Processing table: events
  - Running transformer: events_user_id_transformer (IDDeidentifier)
  - Running transformer: events_datetime_transformer (DateTimeDeidentifier)
Processing table: orders
  - Running transformer: orders_user_id_transformer (IDDeidentifier)
  - Running transformer: orders_datetime_transformer (DateTimeDeidentifier)
De-identification complete!

Results:
  users: (5, 4) -> (5, 3)
  events: (10, 4) -> (10, 4)
  orders: (8, 4) -> (8, 4)
```

## 6. Run De-identification with Engine

Alternatively, use the Cleared Engine programmatically:

```python
import cleared as clr
from cleared.cli.utils import load_config_from_file

# Load configuration
config = load_config_from_file("multi_table_config.yaml")

# Create engine
engine = clr.ClearedEngine.from_config(config)

# Run de-identification
results = engine.run()

print("Multi-table de-identification complete!")
for table_name, result in results.items():
    print(f"{table_name}: {result.transformed_data.shape}")
```

## 7. Verify Results

Check the output files:

```bash
# Check the de-identified data
cat output/users.csv
cat output/events.csv
cat output/orders.csv

# Check the de-identification reference tables
ls deid_ref/
cat deid_ref/user_uid.csv
cat deid_ref/user_uid_shift.csv
```

Expected output for users:
```csv
user_id,reg_date_time,zipcode
2,2017-01-15 10:30:00,10001
4,2020-06-22 14:45:00,90210
5,2021-03-08 09:15:00,60601
1,2018-11-12 16:20:00,33101
3,2020-07-03 11:55:00,98101
```

Expected output for events:
```csv
user_id,event_name,event_value,event_date_time
2,sensor_1,100.0,2020-01-10 08:30:00
2,sensor_2,250.0,2020-01-15 14:20:00
4,sensor_1,50.0,2020-02-05 09:45:00
4,sensor_3,0.0,2020-02-05 17:30:00
5,sensor_1,75.0,2020-03-12 10:15:00
5,sensor_2,300.0,2020-03-12 15:45:00
1,sensor_1,25.0,2020-04-08 11:20:00
3,sensor_1,150.0,2020-05-20 13:10:00
3,sensor_2,400.0,2020-05-25 16:30:00
3,sensor_3,0.0,2020-05-25 18:45:00
```

The results show:
- **user_id**: Consistently de-identified across all tables (1, 2, 3, 4, 5)
- **datetime columns**: Randomly shifted by -5 to +5 years
- **name column**: Dropped from users table
- **Referential integrity**: Maintained across all tables


## 8. Troubleshooting

### Common Issues

1. **Missing dependencies**: Ensure `depends_on` tables are processed first
2. **Inconsistent UIDs**: Use the same `uid` across related tables
3. **Missing columns**: Verify all referenced columns exist in the data
4. **File not found**: Check that input CSV files exist in the correct directory

### Modular Configuration Issues

1. **Tables not appearing**: Check that table config files have `tables:` as the root key
2. **Import files not found**: Verify all files listed in `defaults` exist in the same directory
3. **Configuration not merging**: Ensure YAML syntax is correct in all config files
4. **Missing transformers**: Check that transformer configurations are properly nested under `tables:`

**Debugging modular configuration:**
```python
# Check if configuration loaded correctly
config = load_config_from_file("multi_table_config.yaml")
print(f"Tables loaded: {list(config.tables.keys())}")

# If tables are missing, check individual files
import yaml
with open("users_table_config.yaml", "r") as f:
    users_cfg = yaml.safe_load(f)
    print("Users config structure:", list(users_cfg.keys()))
```

### Debug Mode

Add debug output to see the processing order:

```python
print(f"Processing table: {table_name}")
print(f"  Dependencies: {table_config.depends_on}")
print(f"  Transformers: {[t.uid for t in table_config.transformers]}")
```

## Next Steps

- Learn about [using Cleared with engines](engine_usage.md) for more complex workflows
- Check out [performance optimization](performance_tips.md) for large datasets
- Explore [custom transformers](custom_transformers.md) for specialized de-identification needs
