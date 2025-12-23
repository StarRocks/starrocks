---
displayed_sidebar: docs
---

# get_variant_bool

Extracts a boolean value from a VARIANT object at the specified path.

This function navigates to the specified path in a VARIANT value and returns the value as a BOOLEAN. If the value at the path is not a boolean or cannot be converted to a boolean, the function returns NULL.

## Syntax

```Haskell
BOOLEAN get_variant_bool(variant_expr, path)
```

## Parameters

- `variant_expr`: the expression that represents the VARIANT object. This is typically a VARIANT column from an Iceberg table.

- `path`: the expression that represents the path to an element in the VARIANT object. The value of this parameter is a string. The path syntax is similar to JSON path:
  - `$` represents the root element
  - `.` is used to access object fields
  - `[index]` is used to access array elements (0-based indexing)
  - Field names with special characters can be quoted: `$."field.name"`

## Return value

Returns a BOOLEAN value (TRUE or FALSE).

If the element does not exist, the path is invalid, or the value cannot be converted to a boolean, the function returns NULL.

## Examples

Example 1: Extract a boolean value from the root of a VARIANT.

```SQL
SELECT get_variant_bool(variant_col, '$')
FROM iceberg_catalog.db.table_with_variants;
```

Example 2: Extract a boolean from a nested field.

```SQL
SELECT get_variant_bool(variant_col, '$.user.is_active')
FROM iceberg_catalog.db.table_with_variants;
```

Example 3: Extract a boolean from an array element.

```SQL
SELECT get_variant_bool(variant_col, '$.flags[0]')
FROM iceberg_catalog.db.table_with_variants;
```

Example 4: Extract booleans from nested structures.

```SQL
SELECT
    get_variant_bool(variant_col, '$.settings.enabled') AS enabled,
    get_variant_bool(variant_col, '$.settings.debug') AS debug_mode
FROM iceberg_catalog.db.table_with_variants;
```

Example 5: Use in WHERE clause for filtering.

```SQL
SELECT *
FROM iceberg_catalog.db.table_with_variants
WHERE get_variant_bool(variant_col, '$.is_active') = TRUE;
```

Example 6: Returns NULL when the path does not exist.

```SQL
SELECT get_variant_bool(variant_col, '$.nonexistent.field')
FROM iceberg_catalog.db.table_with_variants;
-- Returns NULL
```

Example 7: Returns NULL when the value is not a boolean.

```SQL
SELECT get_variant_bool(variant_col, '$.name')
FROM iceberg_catalog.db.table_with_variants;
-- Returns NULL if $.name is a string
```

## See also

- [variant_query](variant_query.md): Query and return VARIANT values
- [get_variant_int](get_variant_int.md): Extract integer values from VARIANT
- [get_variant_string](get_variant_string.md): Extract string values from VARIANT
- [get_variant_double](get_variant_double.md): Extract double values from VARIANT
- [VARIANT data type](../../../data-types/semi_structured/VARIANT.md)
