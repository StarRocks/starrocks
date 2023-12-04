---
displayed_sidebar: "English"
---

# multi_distinct_count

## Description

Returns the total number of rows of the `expr`, equivalent to count(distinct expr).

## Syntax

```Haskell
multi_distinct_count(expr)
```

## Parameters

`expr`: the column or expression based on which `multi_distinct_count()` is performed. If `expr` is a column name, the column can be of any data type.

## Return value

Returns a numeric value. If no rows can be found, 0 is returned. This function ignores NULL values.

## Examples

Suppose there is a table named `test`. Query the category and supplier of each order by `id`.

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

Example 1: Count the number of distinct values in the `category` column.

~~~Plain
select multi_distinct_count(category) from test;
+--------------------------------+
| multi_distinct_count(category) |
+--------------------------------+
|                              4 |
+--------------------------------+
~~~

Example 2: Count the number of distinct values in the `supplier` column.

~~~Plain
select multi_distinct_count(supplier) from test;
+--------------------------------+
| multi_distinct_count(supplier) |
+--------------------------------+
|                              5 |
+--------------------------------+
~~~
