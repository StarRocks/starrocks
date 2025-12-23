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

Example 1: Extract double values from a VARIANT column.

```SQL
SELECT get_variant_double(variant_col, '$.temperature') AS temperature
FROM iceberg_catalog.db.measurements
LIMIT 3;
```

```plaintext
+-------------+
| temperature |
+-------------+
| 23.5        |
| 24.2        |
| 22.8        |
+-------------+
```

Example 2: Extract multiple numeric values from nested structures.

```SQL
SELECT
    get_variant_double(variant_col, '$.price') AS price,
    get_variant_double(variant_col, '$.discount') AS discount,
    get_variant_double(variant_col, '$.tax_rate') AS tax_rate
FROM iceberg_catalog.db.products
LIMIT 3;
```

```plaintext
+--------+----------+----------+
| price  | discount | tax_rate |
+--------+----------+----------+
| 99.99  | 0.15     | 0.08     |
| 149.50 | 0.20     | 0.08     |
| 79.95  | 0.10     | 0.08     |
+--------+----------+----------+
```

Example 3: Perform calculations with extracted values.

```SQL
SELECT
    get_variant_double(variant_col, '$.price') AS original_price,
    get_variant_double(variant_col, '$.price') *
    (1 - get_variant_double(variant_col, '$.discount')) AS final_price
FROM iceberg_catalog.db.products
LIMIT 3;
```

```plaintext
+----------------+-------------+
| original_price | final_price |
+----------------+-------------+
| 99.99          | 84.99       |
| 149.50         | 119.60      |
| 79.95          | 71.96       |
+----------------+-------------+
```

Example 4: Use in aggregation queries.

```SQL
SELECT
    AVG(get_variant_double(variant_col, '$.temperature')) AS avg_temp,
    MAX(get_variant_double(variant_col, '$.temperature')) AS max_temp,
    MIN(get_variant_double(variant_col, '$.temperature')) AS min_temp
FROM iceberg_catalog.db.measurements;
```

```plaintext
+----------+----------+----------+
| avg_temp | max_temp | min_temp |
+----------+----------+----------+
| 23.4     | 28.9     | 18.2     |
+----------+----------+----------+
```

Example 5: Returns NULL when the path does not exist or value is not numeric.

```SQL
SELECT
    get_variant_double(variant_col, '$.nonexistent.field') AS missing,
    get_variant_double(variant_col, '$.name') AS not_a_number
FROM iceberg_catalog.db.table_with_variants
LIMIT 1;
```

```plaintext
+---------+---------------+
| missing | not_a_number  |
+---------+---------------+
| NULL    | NULL          |
+---------+---------------+
```

## keyword

GET_VARIANT_DOUBLE,VARIANT
