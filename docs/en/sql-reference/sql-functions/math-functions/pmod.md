---
displayed_sidebar: "English"
---

# pmod

## Description

Returns the positive remainder of `dividend` divided by`divisor`.

## Syntax

```SQL
pmod(dividend, divisor)
```

## Parameters

- `dividend`: the number to be divided.
- `divisor`: the number that divides.

Both `arg1` and `arg2` support the following data types:

- BIGINT
- DOUBLE

> **NOTE**
>
> `dividend` and `divisor` must agree in the data type. StarRocks performs an implicit conversion if they do not agree in the data type.

## Return value

Returns a value of the same data type as the `dividend`. StarRocks returns NULL if `divisor` is specified as 0.

## Examples

```Plain
mysql> select pmod(3.14,3.14);
+------------------+
| pmod(3.14, 3.14) |
+------------------+
|                0 |
+------------------+

mysql> select pmod(3,6);
+------------+
| pmod(3, 6) |
+------------+
|          3 |
+------------+

mysql> select pmod(11,5);
+-------------+
| pmod(11, 5) |
+-------------+
|           1 |
+-------------+

mysql> select pmod(-11,5);
+--------------+
| pmod(-11, 5) |
+--------------+
|            4 |
+--------------+

mysql> SELECT pmod(11,-5);
+--------------+
| pmod(11, -5) |
+--------------+
|           -4 |
+--------------+

mysql> SELECT pmod(-11,-5);
+---------------+
| pmod(-11, -5) |
+---------------+
|            -1 |
+---------------+
```
