# array_agg

## Description

Aggregates the values including `NULL` in a column into an array, and optionally order by some columns.

## Syntax

```Haskell
ARRAY_AGG(col [order by col0 [desc | asc] [nulls first | nulls last] ...])
```

## Parameters

`col`: the column whose values you want to aggregate. Supported data types are BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, VARCHAR, CHAR, DATETIME, and DATE.

`col0`: decide the order of col. There may be more than one order-by columns.


## Return value

Returns a value of the ARRAY data type, optionally sorted by col0... .

## Usage notes

- The order of the elements in an array is random, which means it may be different from the order of the values in the column if without order-by columns, or sorted by order-by columns.
- The data type of the elements in the returned array is the same as the data type of the values in the column.
- Return `NULL` if its input is empty and without group-by columns.

## Examples

Take the following data table as an example:

```Plain%20Text
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
```

Example 1: Group the values in column a and aggregate the values in column pv into an array based on the grouping of column a with ordering by name or not.

```Plain%20Text
mysql> select a, array_agg(pv order by name nulls first) from t group by a;
+------+---------------------------------+
| a    | array_agg(pv ORDER BY name ASC) |
+------+---------------------------------+
|    2 | [334]                           |
|   11 | [33]                            |
|    1 | [4,5,3]                         |
+------+---------------------------------+

mysql> select a, array_agg(pv) from t group by a;
+------+---------------+
| a    | array_agg(pv) |
+------+---------------+
|   11 | [33]          |
|    2 | [334]         |
|    1 | [3,4,5]       |
+------+---------------+
3 rows in set (0.03 sec)
```

Example 2: Aggregate the values in column pv into an array with ordering by name or not.

```Plain%20Text
mysql> select array_agg(pv order by name desc  nulls last) from t;
+----------------------------------+
| array_agg(pv ORDER BY name DESC) |
+----------------------------------+
| [3,4,5,33,334]                   |
+----------------------------------+
1 row in set (0.02 sec)

mysql> select array_agg(pv) from t;
+----------------+
| array_agg(pv)  |
+----------------+
| [3,4,5,33,334] |
+----------------+
1 row in set (0.03 sec)
```

Example 3: Use the WHERE clause when the values in column pv are aggregated into an array. If no data in column pv meets the condition that is specified in the WHERE clause, a `NULL` value is returned.

```Plain%20Text
mysql> select array_agg(pv order by name desc  nulls last) from t where a < 0;
+----------------------------------+
| array_agg(pv ORDER BY name DESC) |
+----------------------------------+
| NULL                             |
+----------------------------------+
1 row in set (0.02 sec)

mysql> select array_agg(pv) from t where a < 0;
+---------------+
| array_agg(pv) |
+---------------+
| NULL          |
+---------------+
1 row in set (0.03 sec)
```

## Keywords

ARRAY_AGG, ARRAY
