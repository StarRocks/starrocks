---
displayed_sidebar: docs
---

# cbrt

## Description

Computes the cube root of the argument.

This function is supported from v3.3 onwards.

## Syntax

```Haskell
DOUBLE cbrt(DOUBLE arg)
```

## Parameters

`arg`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it computes the cube root of the value.

## Return value

Returns a value of the DOUBLE data type. If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select cbrt(8);
+---------+
| cbrt(8) |
+---------+
|       2 |
+---------+

mysql> select cbrt(-8);
+----------+
| cbrt(-8) |
+----------+
|       -2 |
+----------+

mysql> select cbrt(0);
+---------+
| cbrt(0) |
+---------+
|       0 |
+---------+

mysql> select cbrt("");
+----------+
| cbrt('') |
+----------+
|     NULL |
+----------+
```

## keywords

cbrt, cube root
