# truncate

## Description

Rounds the input down to the nearest equal or smaller value with the specified number of places after the decimal point.

## Syntax

```Shell
truncate(arg1,arg2);
```

## Parameter

- `arg1`: the input to be rounded. It supports the following data types:
  - DOUBLE
  - DECIMAL128
- `arg2`:  the number of places to keep after the decimal point. It supports the INT data type.

## Return value

Returns a value of the same data type as `arg1`.

## Examples

```Plain
mysql> select truncate(3.14,1);
+-------------------+
| truncate(3.14, 1) |
+-------------------+
|               3.1 |
+-------------------+
1 row in set (0.00 sec)
```
