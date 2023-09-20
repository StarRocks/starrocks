# array_agg

## Description

Aggregates the values including `NULL` in a column into an array.

## Syntax

```Haskell
ARRAY_AGG(col)
```

## Parameters

`col`: the column whose values you want to aggregate. Supported data types are BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, VARCHAR, CHAR, DATETIME, and DATE.

## Return value

Returns a value of the ARRAY data type.

## Usage notes

- The order of the elements in an array is random, which means it may be different from the order of the values in the column.
- The data type of the elements in the returned array is the same as the data type of the values in the column.

## Examples

Take the following data table as an example:

<<<<<<< HEAD
```Plain%20Text
mysql> select * from test;

+------+------+

| c1   | c2   |

+------+------+

|    1 | a    |

|    1 | b    |

|    2 | c    |

|    2 | NULL |

|    3 | NULL |

+------+------+
=======
```plaintext
mysql> select * from t;
+------+------+------+
| a    | name | pv   |
+------+------+------+
|   11 |      |   33 |
|    2 | NULL |  334 |
|    1 | fzh  |    3 |
|    1 | fff  |    4 |
|    1 | fff  |    5 |
+------+------+------+
>>>>>>> 7ef3022eec ([Doc] fix build errors related to prism (#31536))
```

Example 1: Group the values in column c1 and aggregate the values in column c2 into an array based on the grouping of column c1.

<<<<<<< HEAD
```Plain%20Text
mysql> select c1, array_agg(c2) from test group by c1;
=======
```plaintext
mysql> select a, array_agg(pv order by name nulls first) from t group by a;
+------+---------------------------------+
| a    | array_agg(pv ORDER BY name ASC) |
+------+---------------------------------+
|    2 | [334]                           |
|   11 | [33]                            |
|    1 | [4,5,3]                         |
+------+---------------------------------+
>>>>>>> 7ef3022eec ([Doc] fix build errors related to prism (#31536))

+------+-----------------+

| c1   | array_agg(`c2`) |

+------+-----------------+

|    1 | ["a","b"]       |

|    2 | [null,"c"]      |

|    3 | [null]          |

+------+-----------------+
```

Example 2: Use the WHERE clause when the values in column c2 are aggregated into an array. If no data in column c2 meets the condition that is specified in the WHERE clause, a `NULL` value is returned.

<<<<<<< HEAD
```Plain%20Text
mysql> select array_agg(c2) from test where c1>4;
=======
```plaintext
mysql> select array_agg(pv order by name desc nulls last) from t;
+----------------------------------+
| array_agg(pv ORDER BY name DESC) |
+----------------------------------+
| [3,4,5,33,334]                   |
+----------------------------------+
1 row in set (0.02 sec)
>>>>>>> 7ef3022eec ([Doc] fix build errors related to prism (#31536))

+-----------------+

| array_agg(`c2`) |

<<<<<<< HEAD
+-----------------+
=======
```plaintext
mysql> select array_agg(pv order by name desc nulls last) from t where a < 0;
+----------------------------------+
| array_agg(pv ORDER BY name DESC) |
+----------------------------------+
| NULL                             |
+----------------------------------+
1 row in set (0.02 sec)
>>>>>>> 7ef3022eec ([Doc] fix build errors related to prism (#31536))

| NULL            |

+-----------------+
```

## Keywords

ARRAY_AGG, ARRAY
