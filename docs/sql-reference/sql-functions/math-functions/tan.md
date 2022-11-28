# tan

## Description

Computes the tangent of the argument.

## Syntax

```Haskell
DOUBLE tan(DOUBLE arg)
```

### Parameters

`arg`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it computes the tangent of the value.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select tan(-1);
+---------------------+
| tan(-1)             |
+---------------------+
| -1.5574077246549023 |
+---------------------+

mysql> select tan(0);
+--------+
| tan(0) |
+--------+
|      0 |
+--------+

mysql> select tan(1);
+--------------------+
| tan(1)             |
+--------------------+
| 1.5574077246549023 |
+--------------------+

mysql> select tan("");
+---------+
| tan('') |
+---------+
|    NULL |
+---------+
```

## keyword

TAN