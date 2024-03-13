---
displayed_sidebar: "English"
---

# tanh

## Description

Computes the hyperbolic tangent of the argument.

This function is supported from v3.0.

## Syntax

```Haskell
DOUBLE tanh(DOUBLE arg)
```

### Parameters

`arg`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it computes the hyperbolic tangent of the value.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select tanh(-1);
+---------------------+
| tanh(-1)            |
+---------------------+
| -0.7615941559557649 |
+---------------------+

mysql> select tanh(0);
+---------+
| tanh(0) |
+---------+
|       0 |
+---------+

mysql> select tanh(1);
+--------------------+
| tanh(1)            |
+--------------------+
| 0.7615941559557649 |
+--------------------+

mysql> select tanh("");
+----------+
| tanh('') |
+----------+
|     NULL |
+----------+
```

## keyword

TANH
