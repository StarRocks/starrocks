# if

## Description

If `expr1` evaluates to TRUE, returns `expr2`. Otherwise, returns `expr3`.

## Syntax

```Haskell
if(expr1,expr2,expr3);
```

## Parameters

`expr1`: the condition. It must be a BOOLEAN value.

`expr2`: This value is returned if the condition is true. This expression must evaluate to any of the following data types: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DATETIME, DATE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, VARCHAR, BITMAP, PERCENTILE, HLL, TIME.

`expr3`: This value is returned if the condition is false. The data type is the same as `expr2`.

> `expr2` and `expr3` must agree in data type.

## Return value

The return value has the same type as `expr2`.

## Examples

```Plain Text
mysql> select if(false,1,2);
+-----------------+
| if(FALSE, 1, 2) |
+-----------------+
|               2 |
+-----------------+
1 row in set (0.00 sec)
```
