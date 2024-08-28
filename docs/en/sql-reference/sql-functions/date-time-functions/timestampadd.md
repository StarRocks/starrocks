---
displayed_sidebar: docs
---

# timestampadd

## Description

Adds an integer expression interval to the date or datetime expression `datetime_expr`.

The unit for the interval as mentioned must be one of the following:

MILLISECOND (since 3.2), SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, or YEAR.

## Syntax

```Haskell
DATETIME TIMESTAMPADD(unit, interval, DATETIME datetime_expr)
```

## Parameters

- `datetime_expr`: the DATE or DATETIME value to which you want to add a time interval.
- `interval`: an integer expression that specifies the number of intervals to add.
- `unit`: the unit of the time interval to add. Supported units include MILLISECOND (since 3.2), SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, and YEAR.

## Return value

Returns a value of the same data type as `datetime_expr`.

## Examples

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

MySQL > SELECT TIMESTAMPADD(MILLISECOND,1,'2019-01-02');
+--------------------------------------------+
| timestampadd(MILLISECOND, 1, '2019-01-02') |
+--------------------------------------------+
| 2019-01-02 00:00:00.001000                 |
+--------------------------------------------+
```

## keyword

TIMESTAMPADD
