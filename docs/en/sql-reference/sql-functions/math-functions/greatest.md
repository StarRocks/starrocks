---
displayed_sidebar: docs
---

# greatest

## Description

Returns the largest value from a list of one or more parameters.

Generally, the return value has the same data type as the input.

The parameters are compared based on the following rules:

- NULL is returned if any of the input parameters is NULL.

- If at least one parameter is of the DOUBLE type, all the parameters are compared as DOUBLE values. The same rule applies to the DECIMAL and FLOAT data types.

- If the parameters are a mix of numbers and strings but the strings can be converted into numbers, the parameters are compared as numbers. If the strings cannot be converted into numbers, the parameters are compared as strings.

- If the parameters are characters, they are compared based on the alphabetical order of the first letter.

## Syntax

```Haskell
GREATEST(expr1,...);
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

Example 1: Return the largest value for a single input.

```Plain
mysql> select greatest(3);
+-------------+
| greatest(3) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)
```

Example 2: Return the largest value from a list of values.

```Plain
mysql> select greatest(3,4,5,5,6);
+-------------------------+
| greatest(3, 4, 5, 5, 6) |
+-------------------------+
|                       6 |
+-------------------------+
1 row in set (0.00 sec)
```

Example 3: One parameter is of the DOUBLE type and a DOUBLE value is returned.

```Plain
mysql> select greatest(7,4.5);
+------------------+
| greatest(7, 4.5) |
+------------------+
|              7.0 |
+------------------+
1 row in set (0.05 sec)
```

Example 4: The input parameters are a mix of number and string but the string can be converted into a number. The parameters are compared as numbers.

```Plain
mysql> select greatest(7,'9');
+------------------+
| greatest(7, '9') |
+------------------+
| 9                |
+------------------+
1 row in set (0.04 sec)
```

Example 5: The input parameters are a mix of number and string but the string cannot be converted into a number. The parameters are compared as strings. The string `'1'` is less than `'at'`.

```Plain
select greatest(1,'at');
+-------------------+
| greatest(1, 'at') |
+-------------------+
| at                |
+-------------------+
```

Example 6: The input parameters are characters. The letter `Z` has the largest value.

```Plain
mysql> select greatest('A','B','Z');
+-------------------------+
| greatest('A', 'B', 'Z') |
+-------------------------+
| Z                       |
+-------------------------+
1 row in set (0.00 sec)
```

## Keywords

GREATEST, greatest
