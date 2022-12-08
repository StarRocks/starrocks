# date_sub

## Description

Subtracts the specified time interval from a date.

## Syntax

```Haskell
DATETIME DATE_SUB(DATETIME date,INTERVAL expr type)
```

## Parameters

- `date`: It must be a valid date expression.
- `expr`: the time interval you want to subtract. It must be of the INT type.
- `type`: It can only be set to any of the following values: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.

## Return value

Returns a DATETIME value. If the date does not exist, for example, `2020-02-30`, or if the date is not a DATETIME value, NULL is returned.

## Examples

```Plain Text
select date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-11-28 23:59:59                             |
+-------------------------------------------------+

select date_sub('2010-02-30', INTERVAL 2 DAY);
+----------------------------------------+
| date_sub('2010-02-30', INTERVAL 2 DAY) |
+----------------------------------------+
| NULL                                   |
+----------------------------------------+
```
