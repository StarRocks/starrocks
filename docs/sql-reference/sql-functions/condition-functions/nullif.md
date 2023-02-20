# nullif

## Description

Returns NULL if `expr1` is equal to `expr2`. Otherwise, returns `expr1`.

## Syntax

```Haskell
nullif(expr1,expr2);
```

## Parameters

`expr1`: This expression must evaluate to any of the following data types: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DATETIME, DATE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, VARCHAR, BITMAP, PERCENTILE, HLL, TIME.

`expr2`: An expression that evaluates to the same data type as `expr1`.

> `expr1` and `expr2` must agree in data type.

## Return value

The return value has the same type as `expr1`.

## Examples

```Plain Text
mysql> select nullif(1,2);
+--------------+
| nullif(1, 2) |
+--------------+
|            1 |
+--------------+

select nullif(1,1);
+--------------+
| nullif(1, 1) |
+--------------+
|         NULL |
+--------------+
```
