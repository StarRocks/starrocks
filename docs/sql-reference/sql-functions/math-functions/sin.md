# sin

## Description

Computes the sine of the argument.

## Syntax

```Haskell
DOUBLE sin(DOUBLE arg)
```

### Parameters

`arg`: You can specify only a numeric value. This function converts the numeric value into a DOUBLE value before it computes the sine of the value.

## Return value

Returns a value of the DOUBLE data type.

## Usage notes

If you specify a non-numeric value, this function returns `NULL`.

## Examples

```Plain
mysql> select sin(-1);
+---------------------+
| sin(-1)             |
+---------------------+
| -0.8414709848078965 |
+---------------------+

mysql> select sin(0);
+--------+
| sin(0) |
+--------+
|      0 |
+--------+

mysql> select sin(1);
+--------------------+
| sin(1)             |
+--------------------+
| 0.8414709848078965 |
+--------------------+

mysql> select sin("");
+---------+
| sin('') |
+---------+
|    NULL |
+---------+
```

## keyword

SIN
