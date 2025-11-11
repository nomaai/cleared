# Value Casting in Transformers

The `value_cast` option allows you to specify the data type for the de-identification column before transformation. This is particularly useful when working with CSV files, where numeric values and datetimes are often stored as strings.

## Overview

When data is loaded from CSV files, pandas may infer column types differently than expected. For example:
- Numeric IDs might be read as strings (`"101"` instead of `101`)
- Datetime values might be read as strings (`"2023-01-20 10:15:00"` instead of a datetime object)

The `value_cast` option ensures that the de-identification column is cast to the correct type before the transformer processes it, preventing type mismatch issues and ensuring consistent de-identification mappings.

## Supported Cast Types

The following cast types are currently supported:

- **`integer`**: Converts values to integer type (int64 or Int64 for nullable)
- **`float`**: Converts values to float type (float64)
- **`string`**: Converts values to string type
- **`datetime`**: Converts values to datetime type using pandas `to_datetime()`

## Transformers That Support Value Casting

The following transformers support the `value_cast` option:

1. **`IDDeidentifier`**: Casts the ID column specified in `idconfig.name`
2. **`DateTimeDeidentifier`**: Casts the datetime column specified in `datetime_column`

Both of these transformers inherit from `FilterableTransformer`, which provides the casting functionality.

## Usage Examples

### Example 1: Casting String IDs to Integer

When IDs are stored as strings in CSV files but need to match integer IDs from another table:

```yaml
- method: "IDDeidentifier"
  uid: "events_value_id_transformer"
  configs:
    idconfig:
      name: "event_value"
      uid: "user_uid"
      description: "User ID in event_value"
  filter:
    where_condition: "event_name == 'user submitted'"
  value_cast: "integer"  # Cast string IDs to integer to match user_id type
```

**Why this is needed:**
- If `user_id` in the users table is an integer (101, 202, 303)
- And `event_value` in the events table is a string ("101", "202", "303")
- Without casting, the set comparison in `IDDeidentifier` will treat "101" ≠ 101, creating duplicate mappings
- With `value_cast: "integer"`, both are treated as integers, ensuring consistent de-identification

### Example 2: Casting String Datetimes to Datetime Type

When datetime values are stored as strings in CSV files:

```yaml
- method: "DateTimeDeidentifier"
  uid: "events_value_datetime_transformer"
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
```

**Why this is needed:**
- If datetime values are stored as strings ("2023-01-20 10:15:00")
- The `DateTimeDeidentifier` needs actual datetime objects to perform time shifting
- With `value_cast: "datetime"`, strings are converted to datetime before transformation

### Example 3: Casting to Float

When numeric values need to be treated as floats:

```yaml
- method: "IDDeidentifier"
  uid: "numeric_id_transformer"
  configs:
    idconfig:
      name: "numeric_value"
      uid: "numeric_uid"
      description: "Numeric identifier"
  value_cast: "float"  # Cast to float type
```

### Example 4: Casting to String

When you need to ensure values are treated as strings:

```yaml
- method: "IDDeidentifier"
  uid: "string_id_transformer"
  configs:
    idconfig:
      name: "string_value"
      uid: "string_uid"
      description: "String identifier"
  value_cast: "string"  # Cast to string type
```

## How It Works

1. **Filtering**: The filter is applied first to get the subset of rows that match the condition
2. **Casting**: If `value_cast` is specified, the de-identification column is cast to the specified type
3. **Transformation**: The actual de-identification transformation is applied to the cast values

The casting happens in the `FilterableTransformer` base class, after filtering but before the subclass's `_apply_transform()` method is called.

## Type Conversion Details

### Integer Casting
- Uses `pd.to_numeric()` with `errors="coerce"` to handle non-numeric values
- Uses nullable integer type (`Int64`) if there are any NaN values
- Uses standard integer type (`int64`) if all values are numeric

### Float Casting
- Uses `pd.to_numeric()` with `errors="coerce"` to handle non-numeric values
- Converts to `float64` type

### String Casting
- Uses `.astype(str)` to convert all values to strings

### Datetime Casting
- Uses `pd.to_datetime()` with `errors="coerce"` to handle various datetime formats
- Automatically infers datetime format from the string values
- Handles common formats like "YYYY-MM-DD", "YYYY-MM-DD HH:MM:SS", etc.

## Best Practices

1. **Use `value_cast` when reading from CSV files**: CSV files often store numeric and datetime values as strings
2. **Match types across related columns**: If two columns use the same `uid` for de-identification, ensure they have the same type using `value_cast`
3. **Use `datetime` for datetime columns**: Always use `value_cast: "datetime"` when de-identifying datetime columns from CSV files
4. **Use `integer` for ID columns**: When IDs are stored as strings but should match integer IDs, use `value_cast: "integer"`

## Troubleshooting

### Issue: Duplicate mappings created for the same ID

**Symptom**: The deid_ref_df has duplicate entries for the same ID value (e.g., both "101" and 101).

**Solution**: Add `value_cast: "integer"` (or appropriate type) to ensure consistent type handling.

### Issue: Datetime de-identification not working

**Symptom**: Datetime values are not being shifted correctly.

**Solution**: Add `value_cast: "datetime"` to convert string datetimes to datetime objects before transformation.

### Issue: Type mismatch errors

**Symptom**: Errors about type mismatches during de-identification.

**Solution**: Ensure that columns using the same `uid` have the same type. Use `value_cast` to normalize types.

## Related Documentation

- [Filter-Based De-identification Tutorial](filter_deid_example.md) - See value_cast in action
- [Multi-Table Pipeline Configuration](multi_table_pipeline_config.md) - Learn about multi-table scenarios
- [Transformer Base Classes](../cleared/transformers/base.py) - Technical implementation details

