---
displayed_sidebar: docs
---

# is_nan

Returns whether a floating-point value is NaN (Not a Number).

## Syntax

```Haskell
is_nan(x)
```

### Parameters

- `x`: A floating-point expression. Supported types: FLOAT, DOUBLE.

## Return value

Returns a value of the BOOLEAN data type.

## Usage notes

- If `x` is `NULL`, the return value is `NULL`.
- This function checks IEEE 754 NaN for FLOAT and DOUBLE values.
- In many built-in math functions, invalid-domain inputs yield `NULL` rather than NaN. NaN values most commonly appear when reading external data that contains NaN. Use `is_nan` to filter or diagnose such values.

## Examples

Example 1: Check a normal floating-point value.

```sql
mysql> SELECT is_nan(1.0);
+-------------+
| is_nan(1.0) |
+-------------+
|           0 |
+-------------+
```

Example 2: Propagate NULL.

```sql
mysql> SELECT is_nan(NULL);
+---------------+
| is_nan(NULL)  |
+---------------+
|          NULL |
+---------------+
```

