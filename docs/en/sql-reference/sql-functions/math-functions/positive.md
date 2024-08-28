---
displayed_sidebar: docs
---

# positive

## Description

Returns `x` as a value.

## Syntax

```Haskell
POSITIVE(x);
```

## Parameters

`x`: supports the BIGINT, DOUBLE, DECIMALV2, DECIMAL32, DECIMAL64, and DECIMAL128 data types.

## Return value

Returns a value whose data type is the same as the data type of the `x` value.

## Examples

```Plain
mysql> select positive(3);
+-------------+
| positive(3) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)

mysql> select positive(cast(3.14 as decimalv2));
+--------------------------------------+
| positive(CAST(3.14 AS DECIMAL(9,0))) |
+--------------------------------------+
|                                 3.14 |
+--------------------------------------+
1 row in set (0.01 sec)
```
