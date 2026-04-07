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
- `"Null"` - for NULL values
- `"Boolean(true)"` - for boolean true values
- `"Boolean(false)"` - for boolean false values
- `"Int8"`, `"Int16"`, `"Int32"`, `"Int64"` - for integer values
- `"Float"`, `"Double"` - for floating-point values
- `"Decimal4"`, `"Decimal8"`, `"Decimal16"` - for decimal values with different precision
- `"String"` - for string values
- `"Binary"` - for binary data
- `"Date"` - for date values
- `"TimestampTz"`, `"TimestampNtz"` - for timestamp values with or without timezone
- `"TimestampTzNanos"`, `"TimestampNtzNanos"` - for nanosecond-precision timestamp values
- `"TimeNtz"` - for time values without timezone
- `"Uuid"` - for UUID values
- `"Object"` - for struct or map values
- `"Array"` - for array values

## Examples

Example 1: Get the type of a VARIANT value at the root level.

```SQL
SELECT variant_typeof(data) AS type
FROM bluesky
LIMIT 1;
```

```plaintext
+--------+
| type   |
+--------+
| Object |
+--------+
```

Example 2: Get the type of nested fields.

```SQL
SELECT
    variant_typeof(variant_query(data, '$.kind')) AS kind_type,
    variant_typeof(variant_query(data, '$.did')) AS did_type,
    variant_typeof(variant_query(data, '$.commit')) AS commit_type
FROM bluesky
LIMIT 1;
```

```plaintext
+-----------+----------+-------------+
| kind_type | did_type | commit_type |
+-----------+----------+-------------+
| String    | String   | Object      |
+-----------+----------+-------------+
```

Example 3: Check types of multiple fields.

```SQL
SELECT
    variant_typeof(variant_query(data, '$.commit')) AS commit_type,
    variant_typeof(variant_query(data, '$.time_us')) AS time_type
FROM bluesky
LIMIT 1;
```

```plaintext
+-------------+-----------+
| commit_type | time_type |
+-------------+-----------+
| Object      | Int64     |
+-------------+-----------+
```

Example 4: Use in conditional logic to handle different types.

```SQL
SELECT
    CASE variant_typeof(variant_query(data, '$.time_us'))
        WHEN 'Int64' THEN get_variant_int(data, '$.time_us')
        ELSE NULL
    END AS timestamp_value
FROM bluesky
LIMIT 3;
```

```plaintext
+------------------+
| timestamp_value  |
+------------------+
| 1733267476040329 |
| 1733267476040803 |
| 1733267476041472 |
+------------------+
```

Example 5: Filter rows based on type.

```SQL
SELECT COUNT(*) AS object_count
FROM bluesky
WHERE variant_typeof(variant_query(data, '$.commit')) = 'Object';
```

```plaintext
+--------------+
| object_count |
+--------------+
| 9960624      |
+--------------+
```

## keyword

VARIANT_TYPEOF,VARIANT
