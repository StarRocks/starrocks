# timestampadd

## description

### Syntax

```Haskell
DATETIME TIMESTAMPADD(unit, interval, DATETIME datetime_expr)
```

Timestampadd adds the integer expression interval to the date or datetime expression datetime_expr.

The unit for the interval as mentioned should be one of the following:

SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, or YEAR.

## example

```plain text

MySQL > SELECT TIMESTAMPADD(MINUTE,1,'2019-01-02');
+------------------------------------------------+
| timestampadd(MINUTE, 1, '2019-01-02 00:00:00') |
+------------------------------------------------+
| 2019-01-02 00:01:00                            |
+------------------------------------------------+

MySQL > SELECT TIMESTAMPADD(WEEK,1,'2019-01-02');
+----------------------------------------------+
| timestampadd(WEEK, 1, '2019-01-02 00:00:00') |
+----------------------------------------------+
| 2019-01-09 00:00:00                          |
+----------------------------------------------+
```

## keyword

TIMESTAMPADD
