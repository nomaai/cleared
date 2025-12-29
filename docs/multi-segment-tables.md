# Multi-Segment Tables

Cleared supports processing tables that are split across multiple segment files within a directory. This is useful for large datasets that are stored as multiple files.

## Overview

Tables can be stored in two ways:

1. **Single File**: A single CSV/Parquet/JSON file (e.g., `users.csv`)
2. **Directory with Segments**: A directory containing multiple segment files (e.g., `users/users_segment_1.csv`, `users/users_segment_2.csv`)

Cleared automatically detects whether a table is a single file or a directory of segments and processes them accordingly.

## Directory Structure

For multi-segment tables, create a directory named after the table and place all segment files inside:

```
input/
├── users/              # Table directory
│   ├── users_segment_1.csv   # Segment file 1
│   ├── users_segment_2.csv   # Segment file 2
│   └── users_segment_3.csv   # Segment file 3
└── events.csv         # Single file table
```

**Important Notes:**
- The directory name must match the table name in your configuration
- Segment file names can be anything (they don't need to follow a naming pattern)
- All files within the directory will be processed as segments
- Each segment file must have the same schema (columns)

## Processing Behavior

When processing multi-segment tables:

1. **Forward Transform**: Each segment is processed individually, de-identified, and written to the output directory maintaining the same structure:
   ```
   output/
   └── users/
       ├── users_segment_1.csv
       ├── users_segment_2.csv
       └── users_segment_3.csv
   ```

2. **De-identification Reference**: The de-identification mappings are accumulated across all segments, ensuring consistent de-identification across the entire table.

3. **Reverse Transform**: Segments are read from the output directory and reversed individually, maintaining the segment structure.

4. **Verification**: All segments are combined before comparison to verify reversibility.


## Best Practices

1. **Consistent Schema**: Ensure all segment files have identical column names and types
2. **File Format**: Use the same file format for all segments (all CSV, all Parquet, etc.)
3. **Naming**: While segment file names are flexible, use descriptive names for easier debugging
4. **Testing**: Use `cleared test` with `--rows` limit to verify configuration before full processing

