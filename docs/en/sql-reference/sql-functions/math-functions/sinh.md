---
displayed_sidebar: "English"
---

# sinh

## Description

Computes the hyperbolic sine of the argument.

This function is supported from v3.0.

## Syntax

```Haskell
DOUBLE sinh(DOUBLE arg)
```

### Parameters

`arg`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it computes the hyperbolic sine of the value.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select sinh(-1);
+---------------------+
| sinh(-1)            |
+---------------------+
| -1.1752011936438014 |
+---------------------+

mysql> select sinh(0);
+---------+
| sinh(0) |
+---------+
|       0 |
+---------+

mysql> select sinh(1);
+--------------------+
| sinh(1)            |
+--------------------+
| 1.1752011936438014 |
+--------------------+

mysql> select sinh("");
+----------+
| sinh('') |
+----------+
|     NULL |
+----------+
```

## keyword

SINH
