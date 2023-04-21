
# COUNT

## Description

Returns the total number of rows specified by an expression.

This function has three variations:

- `COUNT(*)` counts all rows in a table, no matter whether they contain NULL values.

- `COUNT(expr)` counts the number of rows with non-NULL values in a specific column.

- `COUNT(DISTINCT expr)` counts the number of distinct non-NULL values in a column.

This function can be used with WHERE and GROUP BY.

`COUNT(DISTINCT expr)` is used for exact count distinct. If you require higher count distinct performance, see [Use bitmap for exact count discount](../../../using_starrocks/Using_bitmap.md).

## Syntax

~~~Haskell
COUNT(expr)
COUNT(DISTINCT expr)`
~~~

## Parameters

`expr`: the column or expression based on which `count()` is performed. If `expr` is a column name, the column can be of any data type.

## Return value

Returns a numeric value. If no rows can be found, 0 is returned. This function ignores NULL values.

## Examples

Suppose there is a table named `test`. Query the country, category, and supplier of each order by `id`.

~~~Plain
select * from test order by id;
+------+----------+----------+------------+
| id   | country  | category | supplier   |
+------+----------+----------+------------+
| 1001 | US       | A        | supplier_1 |
| 1002 | Thailand | A        | supplier_2 |
| 1003 | Turkey   | B        | supplier_3 |
| 1004 | US       | A        | supplier_2 |
| 1005 | China    | C        | supplier_4 |
| 1006 | Japan    | D        | supplier_3 |
| 1007 | Japan    | NULL     | supplier_5 |
+------+----------+----------+------------+
~~~

Example 1: Count the number of rows in table `test`.

~~~Plain
    select count(*) from test;
    +----------+
    | count(*) |
    +----------+
    |        7 |
    +----------+
~~~

Example 2: Count the number of values in the `id` column.

~~~Plain
    select count(id) from test;
    +-----------+
    | count(id) |
    +-----------+
    |         7 |
    +-----------+
~~~

Example 3: Count the number of values in the `category` column while ignoring NULL values.

~~~Plain
select count(category) from test;
  +-----------------+
  | count(category) |
  +-----------------+
  |         6       |
  +-----------------+
~~~

Example 4: Count the number of distinct values in the `category` column.

~~~Plain
select count(distinct category) from test;
+-------------------------+
| count(DISTINCT category) |
+-------------------------+
|                       4 |
+-------------------------+
~~~
