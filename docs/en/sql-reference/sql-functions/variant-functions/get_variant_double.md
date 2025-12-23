---
displayed_sidebar: docs
---

# get_variant_double

Extracts a double-precision floating-point value from a VARIANT object at the specified path.

This function navigates to the specified path in a VARIANT value and returns the value as a DOUBLE. If the value at the path is not a number or cannot be converted to a double, the function returns NULL.

## Syntax

```Haskell
DOUBLE get_variant_double(variant_expr, path)
```

## Parameters

- `variant_expr`: the expression that represents the VARIANT object. This is typically a VARIANT column from an Iceberg table.

- `path`: the expression that represents the path to an element in the VARIANT object. The value of this parameter is a string. The path syntax is similar to JSON path:
  - `$` represents the root element
  - `.` is used to access object fields
  - `[index]` is used to access array elements (0-based indexing)
  - Field names with special characters can be quoted: `$."field.name"`

## Return value

Returns a DOUBLE value.

If the element does not exist, the path is invalid, or the value cannot be converted to a double, the function returns NULL.

## Examples

Example 1: Extract a double value from the root of a VARIANT.

```SQL
SELECT get_variant_double(variant_col, '$')
FROM iceberg_catalog.db.table_with_variants;
```

Example 2: Extract a double from a nested field.

```SQL
SELECT get_variant_double(variant_col, '$.measurements.temperature')
FROM iceberg_catalog.db.table_with_variants;
```

Example 3: Extract a double from an array element.

```SQL
SELECT get_variant_double(variant_col, '$.values[0]')
FROM iceberg_catalog.db.table_with_variants;
```

Example 4: Extract multiple numeric values from nested structures.

```SQL
SELECT
    get_variant_double(variant_col, '$.price') AS price,
    get_variant_double(variant_col, '$.discount') AS discount,
    get_variant_double(variant_col, '$.tax_rate') AS tax_rate
FROM iceberg_catalog.db.table_with_variants;
```

Example 5: Perform calculations with extracted values.

```SQL
SELECT
    get_variant_double(variant_col, '$.price') *
    (1 - get_variant_double(variant_col, '$.discount')) AS final_price
FROM iceberg_catalog.db.table_with_variants;
```

Example 6: Extract from map-like structures.

```SQL
SELECT get_variant_double(variant_col, '$.metrics.avg_score')
FROM iceberg_catalog.db.table_with_variants;
```

Example 7: Returns NULL when the path does not exist.

```SQL
SELECT get_variant_double(variant_col, '$.nonexistent.field')
FROM iceberg_catalog.db.table_with_variants;
-- Returns NULL
```

Example 8: Returns NULL when the value is not numeric.

```SQL
SELECT get_variant_double(variant_col, '$.name')
FROM iceberg_catalog.db.table_with_variants;
-- Returns NULL if $.name is a string
```

## See also

- [variant_query](variant_query.md): Query and return VARIANT values
- [get_variant_int](get_variant_int.md): Extract integer values from VARIANT
- [get_variant_string](get_variant_string.md): Extract string values from VARIANT
- [get_variant_bool](get_variant_bool.md): Extract boolean values from VARIANT
- [VARIANT data type](../../../data-types/semi_structured/VARIANT.md)
