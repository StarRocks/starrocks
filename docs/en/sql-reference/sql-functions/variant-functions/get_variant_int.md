---
displayed_sidebar: docs
---

# get_variant_int

Extracts an integer value from a VARIANT object at the specified path.

This function navigates to the specified path in a VARIANT value and returns the value as a BIGINT. If the value at the path is not an integer or cannot be converted to an integer, the function returns NULL.

## Syntax

```Haskell
BIGINT get_variant_int(variant_expr, path)
```

## Parameters

- `variant_expr`: the expression that represents the VARIANT object. This is typically a VARIANT column from an Iceberg table.

- `path`: the expression that represents the path to an element in the VARIANT object. The value of this parameter is a string. The path syntax is similar to JSON path:
  - `$` represents the root element
  - `.` is used to access object fields
  - `[index]` is used to access array elements (0-based indexing)
  - Field names with special characters can be quoted: `$."field.name"`

## Return value

Returns a BIGINT value.

If the element does not exist, the path is invalid, or the value cannot be converted to an integer, the function returns NULL.

## Examples

Example 1: Extract integer values from a VARIANT column.

```SQL
SELECT get_variant_int(data, '$.time_us') AS timestamp_us
FROM bluesky
LIMIT 3;
```

```plaintext
+------------------+
| timestamp_us     |
+------------------+
| 1733267476040329 |
| 1733267476040803 |
| 1733267476041472 |
+------------------+
```

Example 2: Use in calculations.

```SQL
SELECT
    get_variant_int(data, '$.time_us') / 1000000 AS timestamp_seconds
FROM bluesky
LIMIT 3;
```

```plaintext
+-------------------+
| timestamp_seconds |
+-------------------+
| 1733267476        |
| 1733267476        |
| 1733267476        |
+-------------------+
```

Example 3: Count records by operation type using GROUP BY.

```SQL
SELECT
    get_variant_string(data, '$.commit.operation') AS operation,
    COUNT(*) AS count
FROM bluesky
GROUP BY operation;
```

```plaintext
+-----------+---------+
| operation | count   |
+-----------+---------+
| delete    | 420223  |
| update    | 40283   |
| NULL      | 39361   |
| create    | 9500118 |
+-----------+---------+
```

Example 4: Use in WHERE clause for filtering.

```SQL
SELECT COUNT(*) AS recent_count
FROM bluesky
WHERE get_variant_int(data, '$.time_us') > 1733267476000000;
```

```plaintext
+--------------+
| recent_count |
+--------------+
| 8234567      |
+--------------+
```

Example 5: Returns NULL when the path does not exist or value is not an integer.

```SQL
SELECT
    get_variant_int(data, '$.nonexistent.field') AS missing,
    get_variant_int(data, '$.kind') AS not_an_int
FROM bluesky
LIMIT 1;
```

```plaintext
+---------+-------------+
| missing | not_an_int  |
+---------+-------------+
| NULL    | NULL        |
+---------+-------------+
```

## keyword

GET_VARIANT_INT,GET,VARIANT,INT
