---
displayed_sidebar: "English"
---

# date_sub,subdate

## Description

Subtracts the specified time interval from a date.

## Syntax

```Haskell
DATETIME DATE_SUB(DATETIME|DATE date,INTERVAL expr type)
```

## Parameters

- `date`: It must be a valid DATE or DATETIME expression.
- `expr`: the time interval you want to subtract. It must be of the INT type.
- `type`: the unit of the time interval. It can only be set to any of the following values: YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND (since 3.1.7), and MICROSECOND (since 3.1.7).

## Return value

Returns a DATETIME value. If the date does not exist, for example, `2020-02-30`, or if the date is not a DATE or DATETIME value, NULL is returned.

## Examples

```Plain Text
select date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-11-28 23:59:59                             |
+-------------------------------------------------+

select date_sub('2010-11-30', INTERVAL 2 hour);
+-----------------------------------------+
| date_sub('2010-11-30', INTERVAL 2 HOUR) |
+-----------------------------------------+
| 2010-11-29 22:00:00                     |
+-----------------------------------------+

select date_sub('2010-02-30', INTERVAL 2 DAY);
+----------------------------------------+
| date_sub('2010-02-30', INTERVAL 2 DAY) |
+----------------------------------------+
| NULL                                   |
+----------------------------------------+

select date_sub('2010-11-30 23:59:59', INTERVAL 2 QUARTER);
+-----------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 QUARTER) |
+-----------------------------------------------------+
| 2010-05-30 23:59:59                                 |
+-----------------------------------------------------+

select subdate('2010-11-30 23:59:59', INTERVAL 2 millisecond);
+--------------------------------------------------------+
| subdate('2010-11-30 23:59:59', INTERVAL 2 MILLISECOND) |
+--------------------------------------------------------+
| 2010-11-30 23:59:58.998000                             |
+--------------------------------------------------------+

select date_sub('2010-11-30 23:59:59', INTERVAL 2 microsecond);
+---------------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 MICROSECOND) |
+---------------------------------------------------------+
| 2010-11-30 23:59:58.999998                              |
+---------------------------------------------------------+
```
