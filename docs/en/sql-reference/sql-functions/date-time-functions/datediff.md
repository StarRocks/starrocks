---
displayed_sidebar: docs
---

# datediff

## Description

Calculates the difference between two date values (`expr1 - expr2`) and returns a result in days. `expr1` and `expr2` are valid DATE or DATETIME expressions.

Note: Only the date parts of the values are used in the calculation.

Difference between datediff and [days_diff](./days_diff.md):

|Function|Behavior|Example|
|---|---|---|
|datediff|Accurate to the day|The difference between '2020-12-25 23:00:00' and '2020-12-24 23:00:01' is 1.|
|days_diff|Accurate to the second and rounded down to the nearest lower integer|The difference between '2020-12-25 23:00:00' and '2020-12-24 23:00:01' is 0.|

The difference between datediff and [date_diff](./date_diff.md) lies in that date_diff supports the `unit` parameter, which can return the difference between two date values in the specified unit.

## Syntax

```Haskell
INT DATEDIFF(DATETIME expr1,DATETIME expr2)
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

## References

[date_diff](./date_diff.md)
