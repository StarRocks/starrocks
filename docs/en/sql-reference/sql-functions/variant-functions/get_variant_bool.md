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

Returns a BOOLEAN value (1 or 0).

If the element does not exist, the path is invalid, or the value cannot be converted to a boolean, the function returns NULL.

## Examples

Example 1: Extract a boolean value from a nested field.

```SQL
SELECT get_variant_bool(variant_col, '$.is_active') AS is_active
FROM iceberg_catalog.db.table_with_variants
LIMIT 3;
```

```plaintext
+-----------+
| is_active |
+-----------+
| TRUE      |
| FALSE     |
| TRUE      |
+-----------+
```

Example 2: Extract booleans from nested structures.

```SQL
SELECT
    get_variant_bool(variant_col, '$.settings.enabled') AS enabled,
    get_variant_bool(variant_col, '$.settings.debug') AS debug_mode
FROM iceberg_catalog.db.table_with_variants
LIMIT 3;
```

```plaintext
+---------+------------+
| enabled | debug_mode |
+---------+------------+
| TRUE    | FALSE      |
| TRUE    | TRUE       |
| FALSE   | FALSE      |
+---------+------------+
```

Example 3: Use in WHERE clause for filtering.

```SQL
SELECT COUNT(*) AS active_count
FROM iceberg_catalog.db.table_with_variants
WHERE get_variant_bool(variant_col, '$.is_active') = TRUE;
```

```plaintext
+--------------+
| active_count |
+--------------+
| 1234567      |
+--------------+
```

Example 4: Combine with other variant functions.

```SQL
SELECT
    get_variant_string(variant_col, '$.name') AS name,
    get_variant_bool(variant_col, '$.verified') AS verified
FROM iceberg_catalog.db.table_with_variants
WHERE get_variant_bool(variant_col, '$.verified') = TRUE
LIMIT 5;
```

```plaintext
+------------------+----------+
| name             | verified |
+------------------+----------+
| user123          | TRUE     |
| alice            | TRUE     |
| bob_verified     | TRUE     |
| charlie          | TRUE     |
| trusted_account  | TRUE     |
+------------------+----------+
```

Example 5: Returns NULL when the path does not exist or value is not a boolean.

```SQL
SELECT
    get_variant_bool(variant_col, '$.nonexistent.field') AS missing,
    get_variant_bool(variant_col, '$.name') AS not_a_bool
FROM iceberg_catalog.db.table_with_variants
LIMIT 1;
```

```plaintext
+---------+--------------+
| missing | not_a_bool   |
+---------+--------------+
| NULL    | NULL         |
+---------+--------------+
```

## keyword

GET_VARIANT_BOOL,VARIANT
