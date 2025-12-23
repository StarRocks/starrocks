---
displayed_sidebar: docs
---

# variant_query

Queries the value of an element that can be located by the path expression in a VARIANT object and returns a VARIANT value.

This function is used to navigate and extract sub-elements from VARIANT data read from Iceberg tables in Parquet format.

## Syntax

```Haskell
variant_query(variant_expr, path)
```

## Parameters

- `variant_expr`: the expression that represents the VARIANT object. This is typically a VARIANT column from an Iceberg table.

- `path`: the expression that represents the path to an element in the VARIANT object. The value of this parameter is a string. The path syntax is similar to JSON path:
  - `$` represents the root element
  - `.` is used to access object fields
  - `[index]` is used to access array elements (0-based indexing)
  - Field names with special characters can be quoted: `$."field.name"`

## Return value

Returns a VARIANT value.

If the element does not exist or the path is invalid, the function returns NULL.

## Examples

Example 1: Query the root element of a VARIANT value.

```SQL
SELECT variant_query(variant_col, '$')
FROM iceberg_catalog.db.table_with_variants;
```

Example 2: Query a nested field in a VARIANT object.

```SQL
SELECT variant_query(variant_col, '$.user.profile.name')
FROM iceberg_catalog.db.table_with_variants;
```

Example 3: Query an array element from a VARIANT value.

```SQL
SELECT variant_query(variant_col, '$.scores[0]')
FROM iceberg_catalog.db.table_with_variants;
```

Example 4: Query a field with special characters using quoted field names.

```SQL
SELECT variant_query(variant_col, '$."field.with.dots"')
FROM iceberg_catalog.db.table_with_variants;
```

Example 5: Query nested array elements.

```SQL
SELECT variant_query(variant_col, '$.users[0].addresses[1].city')
FROM iceberg_catalog.db.table_with_variants;
```

Example 6: Cast the result to a specific SQL type.

```SQL
SELECT CAST(variant_query(variant_col, '$.count') AS INT) AS count
FROM iceberg_catalog.db.table_with_variants;
```

Example 7: Query returns NULL when path does not exist.

```SQL
SELECT variant_query(variant_col, '$.nonexistent.field')
FROM iceberg_catalog.db.table_with_variants;
-- Returns NULL
```

## See also

- [get_variant_int](get_variant_int.md): Extract integer values from VARIANT
- [get_variant_string](get_variant_string.md): Extract string values from VARIANT
- [variant_typeof](variant_typeof.md): Get the type of a VARIANT value
- [VARIANT data type](../../../data-types/semi_structured/VARIANT.md)
