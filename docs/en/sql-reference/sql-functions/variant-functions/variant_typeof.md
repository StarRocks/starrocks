---
displayed_sidebar: docs
---

# variant_typeof

Returns the type name of a VARIANT value as a string.

This function inspects a VARIANT value and returns its type as a string. This is useful for understanding the structure of VARIANT data or for conditional processing based on the data type.

## Syntax

```Haskell
VARCHAR variant_typeof(variant_expr)
```

## Parameters

- `variant_expr`: the expression that represents the VARIANT object. This is typically a VARIANT column from an Iceberg table or a VARIANT value returned by `variant_query`.

## Return value

Returns a VARCHAR value representing the type name.

Possible return values include:
- `"boolean"` - for boolean values
- `"int8"`, `"int16"`, `"int32"`, `"int64"` - for integer values
- `"float"`, `"double"` - for floating-point values
- `"decimal"` - for decimal values
- `"string"` - for string values
- `"date"` - for date values
- `"timestamp"`, `"timestamp_ntz"` - for timestamp values
- `"object"` - for struct or map values
- `"array"` - for array values
- `NULL` - if the VARIANT value is NULL

## Examples

Example 1: Get the type of a VARIANT value at the root level.

```SQL
SELECT variant_typeof(variant_col)
FROM iceberg_catalog.db.table_with_variants;
```

Example 2: Get the type of nested fields.

```SQL
SELECT
    variant_typeof(variant_query(variant_col, '$.user')) AS user_type,
    variant_typeof(variant_query(variant_col, '$.age')) AS age_type,
    variant_typeof(variant_query(variant_col, '$.scores')) AS scores_type
FROM iceberg_catalog.db.table_with_variants;
```

Example 3: Use in conditional logic to handle different types.

```SQL
SELECT
    CASE variant_typeof(variant_col)
        WHEN 'int64' THEN get_variant_int(variant_col, '$')
        WHEN 'string' THEN CAST(get_variant_string(variant_col, '$') AS BIGINT)
        ELSE NULL
    END AS value
FROM iceberg_catalog.db.table_with_variants;
```

Example 4: Filter rows based on type.

```SQL
SELECT *
FROM iceberg_catalog.db.table_with_variants
WHERE variant_typeof(variant_query(variant_col, '$.data')) = 'object';
```

Example 5: Identify array fields.

```SQL
SELECT
    id,
    path
FROM iceberg_catalog.db.table_with_variants
WHERE variant_typeof(variant_query(variant_col, '$.items')) = 'array';
```

Example 6: Check types of array elements.

```SQL
SELECT
    variant_typeof(variant_query(variant_col, '$[0]')) AS first_elem_type,
    variant_typeof(variant_query(variant_col, '$[1]')) AS second_elem_type
FROM iceberg_catalog.db.table_with_variants;
```

Example 7: Combine with other functions for type-aware extraction.

```SQL
SELECT
    CASE variant_typeof(variant_query(variant_col, '$.value'))
        WHEN 'int64' THEN CAST(get_variant_int(variant_col, '$.value') AS STRING)
        WHEN 'double' THEN CAST(get_variant_double(variant_col, '$.value') AS STRING)
        WHEN 'string' THEN get_variant_string(variant_col, '$.value')
        WHEN 'boolean' THEN CAST(get_variant_bool(variant_col, '$.value') AS STRING)
        ELSE 'unknown'
    END AS value_as_string
FROM iceberg_catalog.db.table_with_variants;
```

## See also

- [variant_query](variant_query.md): Query and return VARIANT values
- [get_variant_int](get_variant_int.md): Extract integer values from VARIANT
- [get_variant_string](get_variant_string.md): Extract string values from VARIANT
- [get_variant_double](get_variant_double.md): Extract double values from VARIANT
- [get_variant_bool](get_variant_bool.md): Extract boolean values from VARIANT
- [VARIANT data type](../../../data-types/semi_structured/VARIANT.md)
