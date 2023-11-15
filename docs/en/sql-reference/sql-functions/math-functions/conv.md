# conv

## Description

Converts the number `x` from one numeric base system to another, and returns the result as a string value.

## Syntax

```Haskell
CONV(x,y,z);
```

## Parameters

- `x`: the number to be converted. VARCHAR or BIGINT is supported.
- `y`: the source base. TINYINT  is supported.
- `z`: the target base. TINYINT is supported.

## Return value

Returns a value of the VARCHAR data type.

## Examples

Convert the number 8 from numeric base system 2 to numeric base system 10:

```Plain
mysql> select conv(8,10,2);
+----------------+
| conv(8, 10, 2) |
+----------------+
| 1000           |
+----------------+
1 row in set (0.00 sec)
```
