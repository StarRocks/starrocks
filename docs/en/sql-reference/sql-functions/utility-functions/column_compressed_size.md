---
displayed_sidebar: docs
---

# column_compressed_size

Returns the compressed size of a column in bytes. This function is used with the `[_META_]` hint to inspect the metadata of segment files and analyze column storage efficiency.

## Syntax

```SQL
column_compressed_size(column_name)
```

## Parameters

- `column_name`: The name of the column for which you want to get the compressed size.

## Return value

Returns the compressed size of the column in bytes as a BIGINT value.

## Usage notes

- This function must be used with the `[_META_]` hint to access metadata information.
- The function scans the metadata of underlying segment files using the META_SCAN operator.
- Returns the total compressed size of the column data across all segments.
- For complex data types (JSON, ARRAY, MAP, STRUCT), the function recursively calculates the compressed size of all sub-columns.
- The compressed size is calculated by summing the data page sizes via ordinal index ranges.

## Examples

Example 1: Get the compressed size of a simple column.

```sql
SELECT column_compressed_size(id) FROM users [_META_];
```

Example 2: Get the compressed size of complex data types.

```sql
-- Get compressed size of JSON column
SELECT column_compressed_size(json_data) FROM events [_META_];

-- Get compressed size of ARRAY column  
SELECT column_compressed_size(tags) FROM products [_META_];

-- Get compressed size of MAP column
SELECT column_compressed_size(attributes) FROM items [_META_];

-- Get compressed size of STRUCT column
SELECT column_compressed_size(address) FROM customers [_META_];
```

Example 3: Compare compressed sizes across different columns.

```sql
SELECT 
    column_compressed_size(name) as name_compressed_size,
    column_compressed_size(description) as desc_compressed_size,
    column_compressed_size(metadata) as meta_compressed_size
FROM products [_META_];
```

Example 4: Calculate compression ratio.

```sql
SELECT 
    column_name,
    column_size(column_name) as decompressed_size,
    column_compressed_size(column_name) as compressed_size,
    ROUND((1 - column_compressed_size(column_name) / column_size(column_name)) * 100, 2) as compression_ratio_percent
FROM (
    SELECT 'name' as column_name FROM products [_META_]
    UNION ALL
    SELECT 'description' as column_name FROM products [_META_]
    UNION ALL  
    SELECT 'content' as column_name FROM products [_META_]
) t;
```

Example 5: Use with aggregation functions.

```sql
SELECT 
    SUM(column_compressed_size(id)) as total_id_compressed_size,
    AVG(column_compressed_size(content)) as avg_content_compressed_size
FROM articles [_META_];
```

## See also

- [column_size](./column_size.md): Returns the decompressed size of a column.
- [META_SCAN operator](../../../using_starrocks/Cost_based_optimizer.md): For more information about metadata scanning.