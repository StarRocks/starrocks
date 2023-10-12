# datediff

## Description

Calculates the date from `expr1` to `expr2` and returns a result in days.

`expr1` and `expr2` are valid DATE or DATETIME expressions.

Note: Only the date parts of the values are used in the calculation.

<<<<<<< HEAD
=======
Difference between datediff and [days_diff](./days_diff.md):

|Function|Behavior|Example|
|---|---|---|
|datediff|Accurate to the day|The difference between '2020-12-25 23:00:00' and '2020-12-24 23:00:01' is 1.|
|days_diff|Accurate to the second and rounded down to the nearest lower integer|The difference between '2020-12-25 23:00:00' and '2020-12-24 23:00:01' is 0.|

>>>>>>> e1f3ccef8 ([Doc] broken links (#32660))
## Syntax

```Haskell
INT DATEDIFF(DATETIME expr1,DATETIME expr2)`
```

## Examples

```Plain Text
MySQL > select datediff(CAST('2007-12-31 23:59:59' AS DATETIME), CAST('2007-12-30' AS DATETIME));
+-----------------------------------------------------------------------------------+
| datediff(CAST('2007-12-31 23:59:59' AS DATETIME), CAST('2007-12-30' AS DATETIME)) |
+-----------------------------------------------------------------------------------+
|                                                                                 1 |
+-----------------------------------------------------------------------------------+

MySQL > select datediff(CAST('2010-11-30 23:59:59' AS DATETIME), CAST('2010-12-31' AS DATETIME));
+-----------------------------------------------------------------------------------+
| datediff(CAST('2010-11-30 23:59:59' AS DATETIME), CAST('2010-12-31' AS DATETIME)) |
+-----------------------------------------------------------------------------------+
|                                                                               -31 |
+-----------------------------------------------------------------------------------+
```
