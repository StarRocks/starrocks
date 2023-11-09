---
displayed_sidebar: "English"
---

# abs

## Description

Returns the absolute value of the numeric value `x`. If the input value is NULL, NULL is returned.

## Syntax

```Haskell
ABS(x);
```

## Parameters

`x`: the numeric value or expression.

Supported data types: DOUBLE, FLOAT, LARGEINT, BIGINT, INT, SMALLINT, TINYINT, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128.

## Return value

The data type of the return value is the same as the type of `x`.

## Examples

```Plain Text
mysql> select abs(-1);
+---------+
| abs(-1) |
+---------+
|       1 |
+---------+
1 row in set (0.00 sec)
```

## Keywords

abs, absolute
