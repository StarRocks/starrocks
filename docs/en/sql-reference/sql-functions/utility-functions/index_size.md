---
displayed_sidebar: docs
---

# index_size

Returns the size of column indexes in bytes for storage analysis and optimization. This function works with the `[_META_]` hint to inspect segment file metadata.

## Syntax

```SQL
-- Do not omit the brackets [] in the hint.
SELECT index_size(column_name [, index_type]) FROM table_name [_META_];
```

## Parameters

- `column_name`: The name of the column for which you want to get the index size.
- `index_type` (optional): The type of index to query. Valid values are:
  - `'BITMAP'`: Bitmap index size
  - `'BLOOM'`: Bloom filter index size
  - `'ZONEMAP'`: Zone map index size
  - `'ALL'` (default): Total size of all indexes

## Return value

Returns the index size in bytes as a BIGINT value. Returns 0 if the specified index does not exist for the column.

## Usage notes

- This function must be used with the `[_META_]` hint to access metadata information.
- The function scans the metadata of underlying segment files using the META_SCAN operator.
- For complex data types (JSON, ARRAY, MAP, STRUCT), the function recursively calculates the index size of all sub-columns.
- If no `index_type` is specified, `'ALL'` is used by default, which returns the total size of all indexes.
- The function returns 0 for columns that do not have the specified index type.

## Examples

```sql
-- Get the total index size for all indexes on a column
SELECT index_size(city) FROM locations [_META_];

-- Get specific index type sizes
SELECT 
    index_size(city, 'BITMAP') as bitmap_index_size,
    index_size(city, 'BLOOM') as bloom_filter_size,
    index_size(city, 'ZONEMAP') as zonemap_size,
    index_size(city, 'ALL') as total_index_size
FROM locations [_META_];

-- Compare index sizes across multiple columns
SELECT 
    index_size(name) as name_index_size,
    index_size(description) as desc_index_size,
    index_size(category, 'BITMAP') as category_bitmap_size
FROM products [_META_];
```
