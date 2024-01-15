---
displayed_sidebar: "English"
---


# bitxor

## Description

Returns the bitwise XOR of two numeric expressions.

## Syntax

```Haskell
BITXOR(x,y);
```

## Parameters

- `x`: This expression must evaluate to any of the following data types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT.

- `y`: This expression must evaluate to any of the following data types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT.

> `x` and `y` must agree in data type.

## Return value

The return value has the same type as `x`. If any value is NULL, the result is NULL.

## Examples

```Plain Text
mysql> select bitxor(3,0);
+--------------+
| bitxor(3, 0) |
+--------------+
|            3 |
+--------------+
1 row in set (0.00 sec)
```
