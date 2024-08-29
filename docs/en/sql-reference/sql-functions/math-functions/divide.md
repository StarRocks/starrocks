---
displayed_sidebar: docs
---

# divide

## Description

Return the quotient of x divide y. If y is 0, return null.

## Syntax

```Haskell
divide(x, y)
```

### Parameters

- `x`: The supported types are DOUBLE, FLOAT, LARGEINT, BIGINT, INT, SMALLINT, TINYINT, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128.

- `y`: The supported types are the same as `x`.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain Text
mysql> select divide(3, 2);
+--------------+
| divide(3, 2) |
+--------------+
|          1.5 |
+--------------+
1 row in set (0.00 sec)
```
