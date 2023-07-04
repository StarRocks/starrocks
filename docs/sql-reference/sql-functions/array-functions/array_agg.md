# array_agg

## Description

Aggregates values (including `NULL`) in a column into an array (multiple rows to one row), and optionally order the elements by specific columns. From v3.0, array_agg() supports using ORDER BY to sort elements.

## Syntax

```Haskell
ARRAY_AGG(col [order by col0 [desc | asc] [nulls first | nulls last] ...])
```

## Parameters

- `col`: the column whose values you want to aggregate. Supported data types are BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, VARCHAR, CHAR, DATETIME, DATE, ARRAY (since v3.1), MAP (since v3.1), and STRUCT (since v3.1).

- `col0`: the column which decides the order of `col`. There may be more than one ORDER BY column.

- `[desc | asc]`: specifies whether to sort the elements in ascending order (default) or descending order of `col0`.

- `[nulls first | nulls last]`: specifies whether null values are placed at the first or last place.

## Return value

Returns a value of the ARRAY type, optionally sorted by `col0`.

## Usage notes

- The order of the elements in an array is random, which means it may be different from the order of the values in the column if no ORDER BY columns or no sorted by order by columns are specified.
- The data type of the elements in the returned array is the same as the data type of the values in the column.
- Returns `NULL` if the input is empty and without group-by columns.

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

Example 1: Group the values in column `a` and aggregate values in column `pv` into an array by ordering `a` by `name`.

```Plain%20Text
mysql> select a, array_agg(pv order by name nulls first) from t group by a;
+------+---------------------------------+
| a    | array_agg(pv ORDER BY name ASC) |
+------+---------------------------------+
|    2 | [334]                           |
|   11 | [33]                            |
|    1 | [4,5,3]                         |
+------+---------------------------------+

-- Aggregate values with no order.
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

Example 2: Aggregate values in column `pv` into an array with ordering by `name`.

```Plain%20Text
mysql> select array_agg(pv order by name desc nulls last) from t;
+----------------------------------+
| array_agg(pv ORDER BY name DESC) |
+----------------------------------+
| [3,4,5,33,334]                   |
+----------------------------------+
1 row in set (0.02 sec)

-- Aggregate values with no order.
mysql> select array_agg(pv) from t;
+----------------+
| array_agg(pv)  |
+----------------+
| [3,4,5,33,334] |
+----------------+
1 row in set (0.03 sec)
```

Example 3: Aggregate values in column `pv` using the WHERE clause. If no data in `pv` meets the filter condition, a `NULL` value is returned.

```Plain%20Text
mysql> select array_agg(pv order by name desc nulls last) from t where a < 0;
+----------------------------------+
| array_agg(pv ORDER BY name DESC) |
+----------------------------------+
| NULL                             |
+----------------------------------+
1 row in set (0.02 sec)

-- Aggregate values with no order.
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
