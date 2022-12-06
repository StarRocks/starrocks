# timestampdiff

## Description

Returns the interval from `datetime_expr2` to `datetime_expr1`. `datetime_expr1` and `datetime_expr2` must be of the DATE or DATETIME type.

The unit for the integer result and the interval should be one of the following:

SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, or YEAR.

## Syntax

```Haskell
INT TIMESTAMPDIFF(unit,DATETIME datetime_expr1, DATETIME datetime_expr2)
```

## Examples

```plain text
MySQL> SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01');
+--------------------------------------------------------------------+
| timestampdiff(MONTH, '2003-02-01 00:00:00', '2003-05-01 00:00:00') |
+--------------------------------------------------------------------+
|                                                                  3 |
+--------------------------------------------------------------------+

MySQL> SELECT TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01');
+-------------------------------------------------------------------+
| timestampdiff(YEAR, '2002-05-01 00:00:00', '2001-01-01 00:00:00') |
+-------------------------------------------------------------------+
|                                                                -1 |
+-------------------------------------------------------------------+

MySQL> SELECT TIMESTAMPDIFF(MINUTE,'2003-02-01','2003-05-01 12:05:55');
+---------------------------------------------------------------------+
| timestampdiff(MINUTE, '2003-02-01 00:00:00', '2003-05-01 12:05:55') |
+---------------------------------------------------------------------+
|                                                              128885 |
+---------------------------------------------------------------------+

```

## keyword

TIMESTAMPDIFF
