# Linting Rules Reference

This document provides a comprehensive reference for all Cleared configuration linting rules. These rules help ensure your configuration files are valid, consistent, and follow best practices.

## Overview

### Validation Rules

| Rule ID | Name | Description |
|---------|------|-------------|
| cleared-001 | [Required Keys](#cleared-001-required-keys) | Checks that all required top-level keys exist in the configuration file. |
| cleared-002 | [DateTime Requires Time Shift](#cleared-002-datetime-requires-time-shift) | Ensures that if DateTimeDeidentifier is used, the global deid_config has a time_shift configuration. |
| cleared-008 | [DateTime Time Shift Defined](#cleared-008-datetime-time-shift-defined) | Validates that deid_config, time_shift, and time_shift.method are properly defined when DateTimeDeidentifier is used. |
| cleared-012 | [Required Transformer Configs](#cleared-012-required-transformer-configs) | Validates that transformers have all required configuration fields (idconfig, datetime_column, etc.). |

### Dependency Rules

| Rule ID | Name | Description |
|---------|------|-------------|
| cleared-003 | [Unique Transformer UIDs](#cleared-003-unique-transformer-uids) | Ensures transformer UIDs are unique across all tables in the configuration. |
| cleared-004 | [Valid Table Dependencies](#cleared-004-valid-table-dependencies) | Checks that all table names referenced in depends_on actually exist in the configuration. |
| cleared-005 | [Valid Transformer Dependencies](#cleared-005-valid-transformer-dependencies) | Validates that all transformer UIDs referenced in depends_on exist within the same table. |
| cleared-006 | [No Circular Dependencies](#cleared-006-no-circular-dependencies) | Detects circular dependencies in both table dependencies and transformer dependencies. |
| cleared-010 | [Column Dropper Dependencies](#cleared-010-column-dropper-dependencies) | Checks if a ColumnDropper removes columns that other transformers depend on. |

### Uniqueness Rules

| Rule ID | Name | Description |
|---------|------|-------------|
| cleared-015 | [Table Name Consistency](#cleared-015-table-name-consistency) | Ensures table names (the name field) are unique across all tables to prevent confusion. |

### Format Rules

| Rule ID | Name | Description |
|---------|------|-------------|
| cleared-007 | [UID Format](#cleared-007-uid-format) | Validates that transformer UIDs and table names follow proper format (lowercase alphanumeric with underscores). |
| cleared-014 | [Multiple Transformers Same Column](#cleared-014-multiple-transformers-same-column) | Warns if multiple transformers without filters are trying to modify the same column in the same table. |

### Time Shift Rules

| Rule ID | Name | Description |
|---------|------|-------------|
| cleared-009 | [Time Shift Risk Warnings](#cleared-009-time-shift-risk-warnings) | Warns about potential risks associated with specific time shift methods (day-of-week or hour-of-day pattern changes). |
| cleared-011 | [Time Shift Range Validation](#cleared-011-time-shift-range-validation) | Validates time shift range configuration (checks min ≤ max and warns about entirely negative ranges). |

### IO Rules

| Rule ID | Name | Description |
|---------|------|-------------|
| cleared-013 | [IO Configuration Validation](#cleared-013-io-configuration-validation) | Validates IO configuration (io_type, file_format, base_path) and warns if input/output paths are the same. |
| cleared-018 | [Output Paths System Directories](#cleared-018-output-paths-system-directories) | Warns if output paths are in system directories like /tmp, /var, etc. |
| cleared-019 | [Input/Output Path Overlap](#cleared-019-inputoutput-path-overlap) | Warns if input and output paths overlap, which can cause data corruption. |

### Transformer Rules

| Rule ID | Name | Description |
|---------|------|-------------|
| cleared-016 | [Value Cast Appropriateness](#cleared-016-value-cast-appropriateness) | Validates that value_cast is used appropriately for each transformer type. |
| cleared-017 | [Table Has Transformers](#cleared-017-table-has-transformers) | Warns if a table has no transformers, which means no de-identification will be performed. |

### Complexity Rules

| Rule ID | Name | Description |
|---------|------|-------------|
| cleared-020 | [Configuration Complexity](#cleared-020-configuration-complexity) | Warns if configuration file has more than 50 non-empty lines and suggests breaking it into modular files. |

## Validation Rules

### cleared-001: Required Keys

**Severity:** Error

**Description:** Checks that all required top-level keys exist in the configuration file. The required keys are: `name`, `deid_config`, `io`, and `tables`.

**Pass Example:**
```yaml
name: "my_pipeline"
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
tables:
  patients:
    name: "patients"
    depends_on: []
    transformers: []
```

**Fail Example:**
```yaml
# Missing 'tables' key
name: "my_pipeline"
deid_config:
  time_shift:
    method: "shift_by_years"
io:
  data:
    input_config:
      io_type: "filesystem"
```

---

### cleared-002: DateTime Requires Time Shift

**Severity:** Error

**Description:** Checks that if `DateTimeDeidentifier` is used anywhere in the configuration, the global `deid_config` must have a `time_shift` configuration defined.

**Pass Example:**
```yaml
name: "my_pipeline"
deid_config:
  time_shift:
    method: "shift_by_years"
    min: -5
    max: 5
tables:
  encounters:
    transformers:
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        configs:
          datetime_column: "admission_date"
```

**Fail Example:**
```yaml
name: "my_pipeline"
deid_config:
  # Missing time_shift
tables:
  encounters:
    transformers:
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        configs:
          datetime_column: "admission_date"
```

---

### cleared-008: DateTime Time Shift Defined

**Severity:** Error

**Description:** Checks that if `DateTimeDeidentifier` is used, `deid_config`, `time_shift`, and `time_shift.method` are all properly defined.

**Pass Example:**
```yaml
name: "my_pipeline"
deid_config:
  time_shift:
    method: "shift_by_days"
    min: -30
    max: 30
tables:
  encounters:
    transformers:
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        configs:
          datetime_column: "admission_date"
```

**Fail Example:**
```yaml
name: "my_pipeline"
deid_config:
  time_shift:
    # Missing method
    min: -30
    max: 30
tables:
  encounters:
    transformers:
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        configs:
          datetime_column: "admission_date"
```

---

### cleared-012: Required Transformer Configs

**Severity:** Error

**Description:** Validates that transformers have all required configuration fields:
- `IDDeidentifier`: requires `idconfig` with `name` and `uid`
- `DateTimeDeidentifier`: requires `idconfig` and `datetime_column`
- `ColumnDropper`: requires `idconfig` with `name`

**Pass Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        configs:
          idconfig:
            name: "patient_id"
            uid: "patient_uid"
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        configs:
          idconfig:
            name: "patient_id"
            uid: "patient_uid"
          datetime_column: "admission_date"
      - method: "ColumnDropper"
        uid: "name_dropper"
        configs:
          idconfig:
            name: "patient_name"
```

**Fail Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        configs:
          idconfig:
            name: "patient_id"
            # Missing uid
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        configs:
          idconfig:
            name: "patient_id"
            uid: "patient_uid"
          # Missing datetime_column
```

---

## Dependency Rules

### cleared-003: Unique Transformer UIDs

**Severity:** Error

**Description:** Checks that transformer UIDs are unique across all tables. Each transformer UID should only appear once in the entire configuration.

**Pass Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
  encounters:
    transformers:
      - method: "IDDeidentifier"
        uid: "encounter_id_transformer"
```

**Fail Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
  encounters:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"  # Duplicate UID
```

---

### cleared-004: Valid Table Dependencies

**Severity:** Error

**Description:** Checks that all table names referenced in `depends_on` actually exist in the configuration.

**Pass Example:**
```yaml
tables:
  patients:
    name: "patients"
    depends_on: []
  encounters:
    name: "encounters"
    depends_on: ["patients"]
```

**Fail Example:**
```yaml
tables:
  patients:
    name: "patients"
    depends_on: []
  encounters:
    name: "encounters"
    depends_on: ["non_existent_table"]  # Table doesn't exist
```

---

### cleared-005: Valid Transformer Dependencies

**Severity:** Error

**Description:** Checks that all transformer UIDs referenced in `depends_on` exist within the same table.

**Pass Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        depends_on: []
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        depends_on: ["patient_id_transformer"]
```

**Fail Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        depends_on: []
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        depends_on: ["non_existent_transformer"]  # Transformer doesn't exist
```

---

### cleared-006: No Circular Dependencies

**Severity:** Error

**Description:** Detects circular dependencies in both table dependencies and transformer dependencies. Circular dependencies create infinite loops and prevent proper execution order.

**Pass Example:**
```yaml
tables:
  patients:
    name: "patients"
    depends_on: []
  encounters:
    name: "encounters"
    depends_on: ["patients"]
```

**Fail Example:**
```yaml
tables:
  patients:
    name: "patients"
    depends_on: ["encounters"]
  encounters:
    name: "encounters"
    depends_on: ["patients"]  # Circular dependency
```

**Transformer Circular Dependency Fail Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "transformer_a"
        depends_on: ["transformer_b"]
      - method: "DateTimeDeidentifier"
        uid: "transformer_b"
        depends_on: ["transformer_a"]  # Circular dependency
```

---

### cleared-010: Column Dropper Dependencies

**Severity:** Error

**Description:** Checks if a `ColumnDropper` transformer removes a column that another transformer depends on (either as a reference ID column or a datetime column). This prevents data loss by ensuring columns aren't dropped before they're used.

**Pass Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        configs:
          idconfig:
            name: "patient_id"
      - method: "ColumnDropper"
        uid: "name_dropper"
        depends_on: ["patient_id_transformer"]
        configs:
          idconfig:
            name: "patient_name"
```

**Fail Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "ColumnDropper"
        uid: "patient_id_dropper"
        configs:
          idconfig:
            name: "patient_id"
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        depends_on: ["patient_id_dropper"]
        configs:
          idconfig:
            name: "patient_id"  # Column was already dropped!
```

---

## Uniqueness Rules

### cleared-015: Table Name Consistency

**Severity:** Error

**Description:** Checks that table names (the `name` field in each table configuration) are unique across all tables. While dictionary keys are automatically unique, this ensures the `name` field values are also unique to prevent confusion.

**Pass Example:**
```yaml
tables:
  patients_table:
    name: "patients"
  encounters_table:
    name: "encounters"
```

**Fail Example:**
```yaml
tables:
  patients_table:
    name: "patients"
  encounters_table:
    name: "patients"  # Duplicate name
```

---

## Format Rules

### cleared-007: UID Format

**Severity:** Error

**Description:** Validates that transformer UIDs and table names follow the proper format: lowercase alphanumeric characters and underscores, not starting or ending with an underscore.

**Pass Example:**
```yaml
tables:
  patients_table:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer_1"
```

**Fail Example:**
```yaml
tables:
  Patients_Table:  # Invalid: uppercase
    transformers:
      - method: "IDDeidentifier"
        uid: "_patient_id_transformer"  # Invalid: starts with underscore
      - method: "DateTimeDeidentifier"
        uid: "datetime-transformer"  # Invalid: contains hyphen
```

---

### cleared-014: Multiple Transformers Same Column

**Severity:** Warning

**Description:** Warns if multiple transformers without filters are trying to modify the same column in the same table. This can lead to unexpected behavior or data loss.

**Pass Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        configs:
          idconfig:
            name: "patient_id"
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        configs:
          datetime_column: "admission_date"
```

**Fail Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer_1"
        configs:
          idconfig:
            name: "patient_id"
      - method: "IDDeidentifier"
        uid: "patient_id_transformer_2"
        configs:
          idconfig:
            name: "patient_id"  # Same column modified twice!
```

**Note:** This rule only applies to transformers without filters. Transformers with filters can safely modify the same column conditionally.

---

## Time Shift Rules

### cleared-009: Time Shift Risk Warnings

**Severity:** Warning

**Description:** Warns about potential risks associated with specific time shift methods:
- `shift_by_days` / `random_days`: May change day-of-week patterns (e.g., Monday → Friday)
- `shift_by_hours` / `random_hours`: May change hour-of-day patterns (e.g., night shift → day shift)

**Pass Example:**
```yaml
deid_config:
  time_shift:
    method: "shift_by_years"  # No warning
    min: -5
    max: 5
```

**Warning Example:**
```yaml
deid_config:
  time_shift:
    method: "shift_by_days"  # Warning: may change day-of-week patterns
    min: -30
    max: 30
```

**Warning Example:**
```yaml
deid_config:
  time_shift:
    method: "random_hours"  # Warning: may change hour-of-day patterns
    min: -12
    max: 12
```

---

### cleared-011: Time Shift Range Validation

**Severity:** Error (if min > max), Warning (if entirely negative)

**Description:** Validates time shift range configuration:
- **Error:** If `min > max` (invalid range)
- **Warning:** If both `min` and `max` are negative (entire range shifts backward)

**Pass Example:**
```yaml
deid_config:
  time_shift:
    method: "shift_by_years"
    min: -5
    max: 5
```

**Error Example:**
```yaml
deid_config:
  time_shift:
    method: "shift_by_days"
    min: 30
    max: -30  # Error: min > max
```

**Warning Example:**
```yaml
deid_config:
  time_shift:
    method: "shift_by_days"
    min: -365
    max: -1  # Warning: entirely negative range
```

---

## IO Rules

### cleared-013: IO Configuration Validation

**Severity:** Error

**Description:** Validates IO configuration:
- `io_type` must be one of: `"filesystem"`, `"sql"`
- `file_format` must be valid for filesystem IO: `csv`, `parquet`, `json`, `excel`, `xlsx`, `xls`, `pickle`
- `base_path` is required for filesystem IO
- Warns if input and output paths are the same (data loss risk)

**Pass Example:**
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
```

**Fail Example:**
```yaml
io:
  data:
    input_config:
      io_type: "invalid_type"  # Error: invalid io_type
      configs:
        base_path: "./input"
        file_format: "invalid_format"  # Error: invalid file_format
    output_config:
      io_type: "filesystem"
      configs:
        # Error: missing base_path
```

**Warning Example:**
```yaml
io:
  data:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "./data"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./data"  # Warning: same path for input and output
```

---

### cleared-018: Output Paths System Directories

**Severity:** Warning

**Description:** Warns if output paths are in system directories like `/tmp`, `/var`, `/usr`, `/etc`, etc. System directories are typically temporary, may be cleaned up automatically, have restricted permissions, and are not suitable for persistent data storage.

**Pass Example:**
```yaml
io:
  data:
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./output"  # Project-specific directory
  runtime_io_path: "./runtime"
```

**Warning Example:**
```yaml
io:
  data:
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "/tmp/output"  # Warning: system directory
  runtime_io_path: "/var/runtime"  # Warning: system directory
```

---

### cleared-019: Input/Output Path Overlap

**Severity:** Warning

**Description:** Warns if input and output paths overlap (e.g., output path is a subdirectory of input path or vice versa). This can cause data corruption as output files may overwrite input files or be read as input during processing.

**Pass Example:**
```yaml
io:
  data:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "./input"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./output"
  deid_ref:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "./deid_ref_input"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./deid_ref_output"
```

**Warning Example:**
```yaml
io:
  data:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "./data"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./data/output"  # Warning: overlaps with input
  deid_ref:
    input_config:
      io_type: "filesystem"
      configs:
        base_path: "./deid_ref"
    output_config:
      io_type: "filesystem"
      configs:
        base_path: "./deid_ref"  # Warning: same path
```

---

## Transformer Rules

### cleared-016: Value Cast Appropriateness

**Severity:** Error (if unsupported), Warning (if inappropriate)

**Description:** Validates that `value_cast` is used appropriately:
- **Error:** If `value_cast` is used with transformers that don't support it (only `IDDeidentifier` and `DateTimeDeidentifier` support it)
- **Warning:** If `value_cast` seems inappropriate for the transformer type:
  - `IDDeidentifier`: typically uses `"integer"` or `"string"` (warns if `"datetime"` is used)
  - `DateTimeDeidentifier`: typically uses `"datetime"` (warns if `"integer"` or `"float"` is used)

**Pass Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        value_cast: "integer"
        configs:
          idconfig:
            name: "patient_id"
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        value_cast: "datetime"
        configs:
          datetime_column: "admission_date"
```

**Error Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "ColumnDropper"
        uid: "name_dropper"
        value_cast: "string"  # Error: ColumnDropper doesn't support value_cast
        configs:
          idconfig:
            name: "patient_name"
```

**Warning Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        value_cast: "datetime"  # Warning: unusual for ID columns
        configs:
          idconfig:
            name: "patient_id"
      - method: "DateTimeDeidentifier"
        uid: "datetime_transformer"
        value_cast: "integer"  # Warning: unusual for datetime columns
        configs:
          datetime_column: "admission_date"
```

---

### cleared-017: Table Has Transformers

**Severity:** Warning

**Description:** Warns if a table has no transformers. Tables without transformers will not perform any de-identification, which may be unintentional.

**Pass Example:**
```yaml
tables:
  patients:
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        configs:
          idconfig:
            name: "patient_id"
```

**Warning Example:**
```yaml
tables:
  patients:
    name: "patients"
    depends_on: []
    transformers: []  # Warning: no transformers
```

---

## Complexity Rules

### cleared-020: Configuration Complexity

**Severity:** Warning

**Description:** Warns if a configuration file has more than 50 non-empty, non-comment lines. Complex configurations can be difficult to maintain and understand. Suggests breaking the configuration into smaller, modular files using Hydra's defaults functionality.

**Pass Example:**
```yaml
# Small, focused configuration (under 50 lines)
name: "simple_pipeline"
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
tables:
  patients:
    name: "patients"
    depends_on: []
    transformers:
      - method: "IDDeidentifier"
        uid: "patient_id_transformer"
        configs:
          idconfig:
            name: "patient_id"
```

**Warning Example:**
```yaml
# Large configuration with many tables and transformers (over 50 lines)
name: "complex_pipeline"
deid_config:
  time_shift:
    method: "shift_by_years"
    min: -5
    max: 5
# ... many more lines ...
# Consider breaking this into:
# - main_config.yaml
# - patients_table_config.yaml
# - encounters_table_config.yaml
# etc., and using Hydra defaults to import them
```

**Recommendation:** For complex configurations, use Hydra's `defaults` functionality to split the configuration into modular files:

```yaml
# main_config.yaml
name: "my_pipeline"
defaults:
  - patients_table_config
  - encounters_table_config
  - io_config
```

---

## Ignoring Linting Rules

You can ignore specific linting rules using comments in your YAML configuration file. There are two supported formats:

### yamllint-style (Recommended)
```yaml
deid_config:
  time_shift:
    method: "random_days"
    min: 30
    max: -30  # yamllint disable-line rule:cleared-011
```

### noqa-style
```yaml
deid_config:
  time_shift:
    method: "random_days"
    min: 30
    max: -30  # noqa: cleared-011
```

### Ignoring Multiple Rules
```yaml
tables:
  patients:
    transformers: []  # yamllint disable-line rule:cleared-017,cleared-020
```

### Ignoring All Rules on a Line
```yaml
tables:
  patients:
    transformers: []  # noqa
```

---

## Summary

| Rule ID | Category | Severity | Description |
|---------|----------|----------|-------------|
| cleared-001 | Validation | Error | Required top-level keys exist |
| cleared-002 | Validation | Error | DateTime requires time_shift |
| cleared-003 | Dependency | Error | Unique transformer UIDs |
| cleared-004 | Dependency | Error | Valid table dependencies |
| cleared-005 | Dependency | Error | Valid transformer dependencies |
| cleared-006 | Dependency | Error | No circular dependencies |
| cleared-007 | Format | Error | UID format validation |
| cleared-008 | Validation | Error | DateTime time shift defined |
| cleared-009 | Time Shift | Warning | Time shift risk warnings |
| cleared-010 | Dependency | Error | Column dropper dependencies |
| cleared-011 | Time Shift | Error/Warning | Time shift range validation |
| cleared-012 | Validation | Error | Required transformer configs |
| cleared-013 | IO | Error | IO configuration validation |
| cleared-014 | Format | Warning | Multiple transformers same column |
| cleared-015 | Uniqueness | Error | Table name consistency |
| cleared-016 | Transformer | Error/Warning | Value cast appropriateness |
| cleared-017 | Transformer | Warning | Table has transformers |
| cleared-018 | IO | Warning | Output paths system directories |
| cleared-019 | IO | Warning | Input/output path overlap |
| cleared-020 | Complexity | Warning | Configuration complexity |

---

For more information about using the linting functionality, see the [CLI Usage Guide](cli-usage.md#cleared-lint).

## Next Tutorial

Continue to the next tutorial: [Date and Time Shifting](date-and-time-shifting.md)
