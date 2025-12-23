---
displayed_sidebar: docs
---

# get_variant_string

Extracts a string value from a VARIANT object at the specified path.

This function navigates to the specified path in a VARIANT value and returns the value as a VARCHAR. If the value at the path is not a string or cannot be converted to a string, the function returns NULL.

## Syntax

```Haskell
VARCHAR get_variant_string(variant_expr, path)
```

## Parameters

- `variant_expr`: the expression that represents the VARIANT object. This is typically a VARIANT column from an Iceberg table.

- `path`: the expression that represents the path to an element in the VARIANT object. The value of this parameter is a string. The path syntax is similar to JSON path:
  - `$` represents the root element
  - `.` is used to access object fields
  - `[index]` is used to access array elements (0-based indexing)
  - Field names with special characters can be quoted: `$."field.name"`

## Return value

Returns a VARCHAR value.

If the element does not exist, the path is invalid, or the value cannot be converted to a string, the function returns NULL.

## Examples

Example 1: Extract a string value from the root of a VARIANT.

```SQL
SELECT get_variant_string(variant_col, '$')
FROM iceberg_catalog.db.table_with_variants;
```

Example 2: Extract a string from a nested field.

```SQL
SELECT get_variant_string(variant_col, '$.user.name')
FROM iceberg_catalog.db.table_with_variants;
```

Example 3: Extract a string from an array element.

```SQL
SELECT get_variant_string(variant_col, '$.tags[0]')
FROM iceberg_catalog.db.table_with_variants;
```

Example 4: Extract multiple string fields from nested structures.

```SQL
SELECT
    get_variant_string(variant_col, '$.user.first_name') AS first_name,
    get_variant_string(variant_col, '$.user.last_name') AS last_name,
    get_variant_string(variant_col, '$.user.email') AS email
FROM iceberg_catalog.db.table_with_variants;
```

Example 5: Extract strings from deeply nested paths.

```SQL
SELECT get_variant_string(variant_col, '$.address.city.name')
FROM iceberg_catalog.db.table_with_variants;
```

Example 6: Use in WHERE clause for filtering.

```SQL
SELECT *
FROM iceberg_catalog.db.table_with_variants
WHERE get_variant_string(variant_col, '$.status') = 'active';
```

Example 7: Extract from map-like structures.

```SQL
SELECT get_variant_string(variant_col, '$.properties.tier')
FROM iceberg_catalog.db.table_with_variants;
```

Example 8: Extract field with special characters using quoted names.

```SQL
SELECT get_variant_string(variant_col, '$."field.with.dots"')
FROM iceberg_catalog.db.table_with_variants;
```

Example 9: Returns NULL when the path does not exist.

```SQL
SELECT get_variant_string(variant_col, '$.nonexistent.field')
FROM iceberg_catalog.db.table_with_variants;
-- Returns NULL
```

Example 10: Cast numeric value to string.

```SQL
SELECT CAST(get_variant_string(variant_col, '$.id') AS BIGINT)
FROM iceberg_catalog.db.table_with_variants;
```

## See also

- [variant_query](variant_query.md): Query and return VARIANT values
- [get_variant_int](get_variant_int.md): Extract integer values from VARIANT
- [get_variant_double](get_variant_double.md): Extract double values from VARIANT
- [get_variant_bool](get_variant_bool.md): Extract boolean values from VARIANT
- [VARIANT data type](../../../data-types/semi_structured/VARIANT.md)
