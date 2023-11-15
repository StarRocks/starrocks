# acos

## Description

Computes the arc cosine of an argument.

## Syntax

```Haskell
DOUBLE acos(DOUBLE arg)
```

### Parameters

`arg`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it computes the arc cosine of the value.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select acos(-1);
+-------------------+
| acos(-1)          |
+-------------------+
| 3.141592653589793 |
+-------------------+

mysql> select acos(0);
+--------------------+
| acos(0)            |
+--------------------+
| 1.5707963267948966 |
+--------------------+

mysql> select acos(1);
+---------+
| acos(1) |
+---------+
|       0 |
+---------+

mysql> select acos("");
+----------+
| acos('') |
+----------+
|     NULL |
+----------+
```

## keyword

ACOS
