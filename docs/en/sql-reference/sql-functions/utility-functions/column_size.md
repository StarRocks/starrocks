---
displayed_sidebar: docs
---

# column_size

Returns the decompressed size of a column in bytes. This function is used with the `[_META_]` hint to inspect the metadata of segment files and analyze column storage characteristics.

## Syntax

```SQL
column_size(column_name)
```

## Parameters

- `column_name`: The name of the column for which you want to get the decompressed size.

## Return value

Returns the decompressed size of the column in bytes as a BIGINT value.

## Usage notes

- This function must be used with the `[_META_]` hint to access metadata information.
- The function scans the metadata of underlying segment files using the META_SCAN operator.
- Returns the total memory footprint of the column data across all segments.
- For complex data types (JSON, ARRAY, MAP, STRUCT), the function recursively calculates the size of all sub-columns.

## Examples

Example 1: Get the decompressed size of a simple column.

```sql
SELECT column_size(id) FROM users [_META_];
```

Example 2: Get the decompressed size of complex data types.

```sql
-- Get size of JSON column
SELECT column_size(json_data) FROM events [_META_];

-- Get size of ARRAY column  
SELECT column_size(tags) FROM products [_META_];

-- Get size of MAP column
SELECT column_size(attributes) FROM items [_META_];

-- Get size of STRUCT column
SELECT column_size(address) FROM customers [_META_];
```

Example 3: Compare column sizes across different columns.

```sql
SELECT 
    column_size(name) as name_size,
    column_size(description) as desc_size,
    column_size(metadata) as meta_size
FROM products [_META_];
```

Example 4: Use with aggregation functions.

```sql
SELECT 
    SUM(column_size(id)) as total_id_size,
    AVG(column_size(content)) as avg_content_size
FROM articles [_META_];
```

## See also

- [column_compressed_size](./column_compressed_size.md): Returns the compressed size of a column.
- [META_SCAN operator](../../../using_starrocks/Cost_based_optimizer.md): For more information about metadata scanning.