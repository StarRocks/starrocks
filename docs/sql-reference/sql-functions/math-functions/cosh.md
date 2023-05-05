# cosh

## Description

Computes the hyperbolic cosine of the argument.

This function is supported from v3.0.

## Syntax

```Haskell
DOUBLE cosh(DOUBLE arg)
```

### Parameters

`arg`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it computes the hyperbolic cosine of the value.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select cosh(-1);
+--------------------+
| cosh(-1)           |
+--------------------+
| 1.5430806348152437 |
+--------------------+

mysql> select cosh(0);
+---------+
| cosh(0) |
+---------+
|       1 |
+---------+

mysql> select cosh(1);
+--------------------+
| cosh(1)            |
+--------------------+
| 1.5430806348152437 |
+--------------------+

mysql> select cosh("");
+----------+
| cosh('') |
+----------+
|     NULL |
+----------+
```

## keyword

COSH
