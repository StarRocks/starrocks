---
displayed_sidebar: "English"
---

# least

## Description

Returns the smallest value from a list of one or more parameters.

Generally, the return value has the same data type as the input.

The comparison rules are the same as the [greatest](greatest.md) function.

## Syntax

```Haskell
LEAST(expr1,...);
```

## Parameters

`expr1`: the expression to compare. It supports the following data types:

- SMALLINT

- TINYINT

- INT

- BIGINT

- LARGEINT

- FLOAT

- DOUBLE

- DECIMALV2

- DECIMAL32

- DECIMAL64

- DECIMAL128

- DATETIME

- VARCHAR

## Examples

Example 1: Return the smallest value for a single input.

```Plain
select least(3);
+----------+
| least(3) |
+----------+
|        3 |
+----------+
1 row in set (0.00 sec)
```

Example 2: Return the smallest value from a list of values.

```Plain
select least(3,4,5,5,6);
+----------------------+
| least(3, 4, 5, 5, 6) |
+----------------------+
|                    3 |
+----------------------+
1 row in set (0.01 sec)
```

Example 3: One parameter is of the DOUBLE type and a DOUBLE value is returned.

```Plain
select least(4,4.5,5.5);
+--------------------+
| least(4, 4.5, 5.5) |
+--------------------+
|                4.0 |
+--------------------+
```

Example 4: The input parameters are a mix of number and string but the string can be converted into a number.  The parameters are compared as numbers.

```Plain
select least(7,'5');
+---------------+
| least(7, '5') |
+---------------+
| 5             |
+---------------+
1 row in set (0.01 sec)
```

Example 5: The input parameters are a mix of number and string but the string cannot be converted into a number. The parameters are compared as strings. The string `'1'` is less than `'at'`.

```Plain
select least(1,'at');
+----------------+
| least(1, 'at') |
+----------------+
| 1              |
+----------------+
```

Example 6: The input parameters are characters.

```Plain
mysql> select least('A','B','Z');
+----------------------+
| least('A', 'B', 'Z') |
+----------------------+
| A                    |
+----------------------+
1 row in set (0.00 sec)
```

## Keywords

LEAST, least
