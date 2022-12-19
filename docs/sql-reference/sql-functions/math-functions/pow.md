# pow, power, dpow, fpow

## Description

Returns the value of `x` raised to the power of `y`.

## Syntax

```SQL
POW(x,y);
POWER(x,y);
```

## Parameters

- `x`: The data type is DOUBLE.
- `y`: The data type is DOUBLE.

## Return value

Returns a value of the DOUBLE data type.

## Example

```
mysql> select pow(2,2);
+-----------+
| pow(2, 2) |
+-----------+
|         4 |
+-----------+
1 row in set (0.00 sec)

mysql> select power(4,3);
+-------------+
| power(4, 3) |
+-------------+
|          64 |
+-------------+
1 row in set (0.00 sec)
```
