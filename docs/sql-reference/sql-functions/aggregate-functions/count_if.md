
# count_if

## Description

Returns the total number of rows that meet the condition specified.

This function does not support `DISTINCT`, e.g., `count_if(DISTINCT x)` is not valid.

This function is internally transformed to `COUNT` + `IF`:

Before: `COUNT_IF(x)`  
After: `COUNT(IF(x, 1, NULL))`

## Syntax

~~~Haskell
COUNT_IF(expr)
~~~

## Parameters

`expr`: the column or expression based on which `count_if()` is performed. If `expr` is a column name, the column can be of any data type.

## Return value

Returns a numeric value. If no rows can be found, 0 is returned. This function ignores NULL values.

## Examples

Suppose there is a table named `test_count_if`.

~~~Plain
select * from test_count_if;
+------+------+---------------------+------+
| v1   | v2   | v3                  | v4   |
+------+------+---------------------+------+
| a    | NULL | 2022-04-18 02:05:00 |    1 |
| a    | a    | 2022-04-18 01:01:00 |    1 |
| a    | b    | 2022-04-18 02:01:00 | NULL |
| a    | b    | 2022-04-18 02:15:00 |    3 |
| a    | b    | 2022-04-18 03:15:00 |    7 |
| c    | NULL | 2022-04-18 03:25:00 |    2 |
| c    | NULL | 2022-04-18 03:45:00 | NULL |
| c    | a    | 2022-04-18 03:27:00 |    3 |
+------+------+---------------------+------+
~~~

Example 1: Count the number of rows in table `test_count_if` where `v2` is null.

~~~Plain
select count_if(v2 is null) from test_count_if;
+----------------------+
| count_if(v2 IS NULL) |
+----------------------+
|                    3 |
+----------------------+
~~~

Example 2: Count the number of rows where `v1 >= v2 or v4 = 1`

~~~Plain
select count_if(v1 >= v2 or v4 = 1)from test_count_if;
+----------------------------------+
| count_if((v1 >= v2) OR (v4 = 1)) |
+----------------------------------+
|                                3 |
+----------------------------------+
~~~