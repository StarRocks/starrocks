---
displayed_sidebar: "English"
---

# asin

## Description

Computes the arc sine of the argument.

## Syntax

```Haskell
DOUBLE asin(DOUBLE arg)
```

### Parameters

`arg`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it computes the arc sine of the value.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select asin(1);
+--------------------+
| asin(1)            |
+--------------------+
| 1.5707963267948966 |
+--------------------+

mysql> select asin(-0.5);
+---------------------+
| asin(-0.5)          |
+---------------------+
| -0.5235987755982989 |
+---------------------+

mysql> select asin(0);
+---------+
| asin(0) |
+---------+
|       0 |
+---------+

mysql> select asin("");
+----------+
| asin('') |
+----------+
|     NULL |
+----------+
```

## keyword

ASIN
