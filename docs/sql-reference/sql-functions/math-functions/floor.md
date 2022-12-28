# floor, dfloor

## Description

Returns the largest integer that is not more than `x`.

## Syntax

```SQL
FLOOR(x);
```

## Parameters

`x`: DOUBLE is supported.

## Return value

Returns a value of the BIGINT data type.

## Examples

```Plaintext
mysql> select floor(3.14);
+-------------+
| floor(3.14) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)
```
