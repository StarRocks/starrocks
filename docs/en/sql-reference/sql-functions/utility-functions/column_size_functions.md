---
displayed_sidebar: docs
---

# column_size & column_compressed_size

These functions return the size information of table columns for storage analysis and optimization. Both functions work with the `[_META_]` hint to inspect segment file metadata.

## column_size

Returns the decompressed size of a column in bytes.

### Syntax

```SQL
column_size(column_name)
```

### Parameters

- `column_name`: The name of the column for which you want to get the decompressed size.

### Return value

Returns the decompressed size of the column in bytes as a BIGINT value.

## column_compressed_size

Returns the compressed size of a column in bytes.

### Syntax

```SQL
column_compressed_size(column_name)
```

### Parameters

- `column_name`: The name of the column for which you want to get the compressed size.

### Return value

Returns the compressed size of the column in bytes as a BIGINT value.

## Usage notes

- Both functions must be used with the `[_META_]` hint to access metadata information.
- The functions scan the metadata of underlying segment files using the META_SCAN operator.
- For complex data types (JSON, ARRAY, MAP, STRUCT), the functions recursively calculate the size of all sub-columns.
- `column_size` returns the total memory footprint of the column data across all segments.
- `column_compressed_size` returns the total compressed size calculated by summing data page sizes via ordinal index ranges.

## Examples

### Basic usage

```sql
-- Get decompressed size of a column
SELECT column_size(id) FROM users [_META_];

-- Get compressed size of a column
SELECT column_compressed_size(id) FROM users [_META_];
```

### Complex data types

```sql
-- Get sizes of JSON column
SELECT 
    column_size(json_data) as decompressed_size,
    column_compressed_size(json_data) as compressed_size
FROM events [_META_];

-- Get sizes of ARRAY column  
SELECT 
    column_size(tags) as decompressed_size,
    column_compressed_size(tags) as compressed_size
FROM products [_META_];

-- Get sizes of MAP column
SELECT 
    column_size(attributes) as decompressed_size,
    column_compressed_size(attributes) as compressed_size
FROM items [_META_];

-- Get sizes of STRUCT column
SELECT 
    column_size(address) as decompressed_size,
    column_compressed_size(address) as compressed_size
FROM customers [_META_];
```

### Compare column sizes

```sql
SELECT 
    column_size(name) as name_decompressed,
    column_compressed_size(name) as name_compressed,
    column_size(description) as desc_decompressed,
    column_compressed_size(description) as desc_compressed,
    column_size(metadata) as meta_decompressed,
    column_compressed_size(metadata) as meta_compressed
FROM products [_META_];
```

### Calculate compression ratio

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

### Use with aggregation functions

```sql
SELECT 
    SUM(column_size(id)) as total_id_decompressed_size,
    SUM(column_compressed_size(id)) as total_id_compressed_size,
    AVG(column_size(content)) as avg_content_decompressed_size,
    AVG(column_compressed_size(content)) as avg_content_compressed_size
FROM articles [_META_];
```

### Storage analysis example

```sql
-- Analyze storage efficiency across all columns
SELECT 
    'users' as table_name,
    column_size(id) as id_decompressed,
    column_compressed_size(id) as id_compressed,
    column_size(name) as name_decompressed,
    column_compressed_size(name) as name_compressed,
    column_size(email) as email_decompressed,
    column_compressed_size(email) as email_compressed,
    (column_size(id) + column_size(name) + column_size(email)) as total_decompressed,
    (column_compressed_size(id) + column_compressed_size(name) + column_compressed_size(email)) as total_compressed,
    ROUND((1 - (column_compressed_size(id) + column_compressed_size(name) + column_compressed_size(email)) / 
           (column_size(id) + column_size(name) + column_size(email))) * 100, 2) as overall_compression_ratio
FROM users [_META_];
```

## See also

- [META_SCAN operator](../../../using_starrocks/Cost_based_optimizer.md): For more information about metadata scanning.