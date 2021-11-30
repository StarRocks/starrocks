# datediff

## description

### Syntax

`DATETIME DATEDIFF(DATETIME expr1,DATETIME expr2)`

Calculate the date from expr 1 to expr 2 and return with a result in days.

Expr 1 and expr 2 are valid date or date-and-time expressions.

Note: Only the date parts of the values are used in the calculation.

## example

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

## keyword

DATEDIFF
