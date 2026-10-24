---
displayed_sidebar: docs
description: "Returns whether a VARIANT value is a VARIANT null, such as a JSON null."
---

# is_variant_null

Returns whether a VARIANT value is a VARIANT null, such as a JSON null.

A VARIANT null is a value: a field that exists and holds `null`. It is different from SQL NULL, which `variant_query` returns when the path does not exist. Use this function to tell the two cases apart.

## Syntax

```Haskell
BOOLEAN is_variant_null(variant_expr)
```

## Parameters

- `variant_expr`: the expression that represents the VARIANT object. This is typically a VARIANT column from an Iceberg table or a VARIANT value returned by `variant_query`.

## Return value

Returns a BOOLEAN value:

- `true` if the value is a VARIANT null.
- `false` for any other value, and for SQL NULL. The function never returns NULL.

| Value at the path | `variant_query(v, path)` | `variant_typeof(...)` | `is_variant_null(...)` |
| ----------------- | ------------------------ | --------------------- | ---------------------- |
| `null`            | VARIANT null             | `Null`                | `true`                 |
| Any other value   | VARIANT value            | Type name             | `false`                |
| Path not found    | SQL NULL                 | NULL                  | `false`                |

## Examples

Example 1: Distinguish a field that holds `null` from a field that does not exist.

```SQL
SELECT
    is_variant_null(variant_query(PARSE_JSON('{"a": null}'), '$.a')) AS json_null,
    is_variant_null(variant_query(PARSE_JSON('{"a": 1}'), '$.a')) AS has_value,
    is_variant_null(variant_query(PARSE_JSON('{"b": 1}'), '$.a')) AS missing;
```

```plaintext
+-----------+-----------+---------+
| json_null | has_value | missing |
+-----------+-----------+---------+
|         1 |         0 |       0 |
+-----------+-----------+---------+
```

Example 2: Count rows whose field `a` exists and holds `null`.

```SQL
SELECT COUNT(*)
FROM t
WHERE is_variant_null(variant_query(v, '$.a'));
```

## keyword

IS_VARIANT_NULL,VARIANT
