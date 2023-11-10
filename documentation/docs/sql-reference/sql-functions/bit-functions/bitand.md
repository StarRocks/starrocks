---
displayed_sidebar: "English"
---

# bitand

## Description

Returns the bitwise AND of two numeric expressions.

## Syntax

```Haskell
BITAND(x,y);
```

## Parameters

- `x`: This expression must evaluate to any of the following data types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT.

- `y`: This expression must evaluate to any of the following data types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT.

> `x` and `y` must agree in data type.

## Return value

The return value has the same type as `x`. If any value is NULL, the result is NULL.

## Examples

```Plain Text
mysql> select bitand(3,0);
+--------------+
| bitand(3, 0) |
+--------------+
|            0 |
+--------------+
1 row in set (0.01 sec)
```
