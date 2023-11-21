---
displayed_sidebar: "English"
---

# pow, power, dpow, fpow

## Description

Returns the result of `x` raised to the power of `y`.

## Syntax

```Haskell
POW(x,y);POWER(x,y);
```

## Parameters

`x`: supports the DOUBLE data type.

`y`: supports the DOUBLE data type.

## Return value

Returns a value of the DOUBLE data type.

## Examples

```Plain
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
