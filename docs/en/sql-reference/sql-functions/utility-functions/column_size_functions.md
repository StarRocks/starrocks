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

```sql
-- Get both decompressed and compressed sizes of columns
SELECT 
    column_size(name) as name_decompressed_size,
    column_compressed_size(name) as name_compressed_size,
    column_size(description) as desc_decompressed_size,
    column_compressed_size(description) as desc_compressed_size
FROM products [_META_];
```

## See also

- [META_SCAN operator](../../../using_starrocks/Cost_based_optimizer.md): For more information about metadata scanning.