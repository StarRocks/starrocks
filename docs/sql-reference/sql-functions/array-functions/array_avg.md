# array_avg

## Description

Calculates the average value of all data in an ARRAY and return this result.

## Syntax

```Haskell
array_avg(array(type))
```

`array(type)` supports the following types of elements: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMALV2.

## Examples

```plain text
mysql> select array_avg([11, 11, 12]);
+-----------------------+
| array_avg([11,11,12]) |
+-----------------------+
| 11.333333333333334    |
+-----------------------+

mysql> select array_avg([11.33, 11.11, 12.324]);
+---------------------------------+
| array_avg([11.33,11.11,12.324]) |
+---------------------------------+
| 11.588                          |
+---------------------------------+
```

## keyword

ARRAY_AVG,ARRAY
