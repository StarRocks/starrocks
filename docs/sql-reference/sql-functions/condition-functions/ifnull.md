# ifnull

## Description

If `expr1` is NULL, returns expr2. If `expr1` is not NULL, returns `expr1`.

## Syntax

```Haskell
ifnull(expr1,expr2);
```

## Parameters

`expr1`: This expression must evaluate to any of the following data types: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DATETIME, DATE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, VARCHAR, BITMAP, PERCENTILE, HLL, TIME.

`expr2`: This expression must evaluate to any of the following data types: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DATETIME, DATE, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, VARCHAR, BITMAP, PERCENTILE, HLL, TIME.

> `expr1` and `expr2` must agree in data type.

## Return value

The return value has the same type as `expr1`.

## Examples

```Plain Text
mysql> select ifnull(NULL,2);
+-----------------+
| ifnull(NULL, 2) |
+-----------------+
|               2 |
+-----------------+
1 row in set (0.01 sec)
```
