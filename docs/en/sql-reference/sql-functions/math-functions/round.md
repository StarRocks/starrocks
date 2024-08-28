---
displayed_sidebar: docs
---

# round, dround

## Description

Rounds a number to a specified number of digits.

- If `n` is not specified, `x` is rounded to the nearest integer.
- If `n` is specified, `x` is rounded to `n` decimal places. If `n` is negative, `x` is rounded to the left of the decimal point. If overflow occurs, an error is returned.

## Syntax

```Haskell
ROUND(x [,n]);
```

## Parameters

`x`: the number to be rounded. It supports the DOUBLE and DECIMAL128 data types.

`n`: the number of decimal places to round the number to. It supports the INT data type. This parameter is optional.

## Return value

- If only `x` is specified, the return value is of the following data type:

  ["DECIMAL128"] -> "DECIMAL128"

  ["DOUBLE"] -> "BIGINT"

- If both `x` and `n` are specified, the return value is of the following data type:

  ["DECIMAL128", "INT"] -> "DECIMAL128"

  ["DOUBLE", "INT"] -> "DOUBLE"

## Examples

```Plain
mysql> select round(3.14);
+-------------+
| round(3.14) |
+-------------+
|           3 |
+-------------+

mysql> select round(3.14,1);
+----------------+
| round(3.14, 1) |
+----------------+
|            3.1 |
+----------------+

mysql> select round(13.14,-1);
+------------------+
| round(13.14, -1) |
+------------------+
|               10 |
+------------------+

mysql> select round(122.14,-1);
+-------------------+
| round(122.14, -1) |
+-------------------+
|               120 |
+-------------------+

mysql> select round(122.14,-2);
+-------------------+
| round(122.14, -2) |
+-------------------+
|               100 |
+-------------------+

mysql> select round(122.14,-3);
+-------------------+
| round(122.14, -3) |
+-------------------+
|                 0 |
+-------------------+
```
