---
displayed_sidebar: "English"
---

# sqrt, dsqrt

## Description

Calculates the square root of a value. dsqrt is same as sqrt.

## Syntax

```Haskell
DOUBLE SQRT(DOUBLE x);
DOUBLE DSQRT(DOUBLE x);
```

## Parameters

`x`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before calculation.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select sqrt(3.14);
+-------------------+
| sqrt(3.14)        |
+-------------------+
| 1.772004514666935 |
+-------------------+
1 row in set (0.01 sec)


mysql> select dsqrt(3.14);
+-------------------+
| dsqrt(3.14)       |
+-------------------+
| 1.772004514666935 |
+-------------------+
```
