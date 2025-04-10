---
displayed_sidebar: docs
---

# negative

## Description

Returns the negative of input `arg`.

## Syntax

```Plain
negative(arg)
```

## Parameter

`arg` supports the following data types:

- BIGINT
- DOUBLE
- DECIMALV2
- DECIMAL32
- DECIMAL64
- DECIMAL128

## Return value

Returns a value of the same data type as the input.

## Examples

```Plain
mysql> select negative(3);
+-------------+
| negative(3) |
+-------------+
|          -3 |
+-------------+
1 row in set (0.00 sec)

mysql> select negative(cast(3.14 as decimalv2));
+--------------------------------------+
| negative(CAST(3.14 AS DECIMAL(9,0))) |
+--------------------------------------+
|                                -3.14 |
+--------------------------------------+
1 row in set (0.01 sec)
```
