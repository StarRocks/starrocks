# COUNT_IF

## Description

Returns the total number of rows that satisfy the expression.

## Syntax

```Haskell
COUNT_IF(expr)
```

## Parameters

`expr`: the column or expression based on which `count_if` is performed. If `expr` is a column name, the column should be numeric.


## Return value

Returns a numeric value. If no rows can be found, 0 is returned. This function ignores NULL values.


## Examples

```plain text
MySQL > SELECT * FROM score_table ORDER BY id;
+------+------------+--------+-------+
| id   | name       | gender | score |
+------+------------+--------+-------+
|    1 | lily       | F      |   100 |
|    2 | alice      | F      |  95.5 |
|    3 | ella       | F      |    83 |
|    4 | tony       | M      |  98.5 |
|    5 | bob        | M      |    94 |
|    6 | tom        | M      |  NULL |
+------+------------+--------+-------+

MySQL > SELECT count_if(score > 0) FROM score_table;
+---------------------+
| count_if(score > 0) |
+---------------------+
|                   5 |
+---------------------+

MySQL > SELECT gender, count_if(score > 95) FROM score_table GROUP BY gender;
+--------+----------------------+
| gender | count_if(score > 95) |
+--------+----------------------+
| F      |                    2 |
| M      |                    1 |
+--------+----------------------+
```

## keyword

COUNT_IF
