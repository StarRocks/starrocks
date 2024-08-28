---
displayed_sidebar: docs
---

# mod

## Description

The modulus function that returns the remainder of `dividend` divided by `divisor`.

## Syntax

```SQL
mod(dividend, divisor)
```

## Parameters

- `dividend`: The number to be divided.
- `divisor`: The number that divides.

Both `dividend` and `divisor` support the following data types:

- TINYINT
- SMALLINT
- INT
- BIGINT
- LARGEINT
- FLOAT
- DOUBLE
- DECIMALV2
- DECIMAL32
- DECIMAL64
- DECIMAL128

> **NOTE**
>
> `dividend` and `divisor` must agree in the data type. StarRocks performs an implicit conversion if they do not agree in the data type.

## Return value

Returns a value of the same data type as the `dividend`. StarRocks returns NULL if `divisor` is specified as 0.

## Examples

```Plain
mysql> select mod(3.14,3.14);
+-----------------+
| mod(3.14, 3.14) |
+-----------------+
|               0 |
+-----------------+

mysql> select mod(3.14, 3);
+--------------+
| mod(3.14, 3) |
+--------------+
|         0.14 |
+--------------+

select mod(11,-5);
+------------+
| mod(11, -5)|
+------------+
|          1 |
+------------+

select mod(-11,5);
+-------------+
| mod(-11, 5) |
+-------------+
|          -1 |
+-------------+
```
