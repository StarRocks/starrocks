# date_add

## Description

Adds a specified time interval to a date.

## Syntax

```Haskell
DATETIME DATE_ADD(DATETIME|DATE date,INTERVAL expr type)
```

## Parameters

- `date`: It must be a valid date or datetime expression.
- `expr`: the time interval you want to add. It must be of the INT type.
- `type`: the unit of the time interval. It can only be set to any of the following values: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.

## Return value

Returns a DATETIME value. If the date does not exist, for example, `2020-02-30`, NULL is returned. If the date is a DATE value, it will be converted into a DATETIME value.

## Examples

```Plain Text
select date_add('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_add('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-12-02 23:59:59                             |
+-------------------------------------------------+

select date_add('2010-12-03', INTERVAL 2 DAY);
+----------------------------------------+
| date_add('2010-12-03', INTERVAL 2 DAY) |
+----------------------------------------+
| 2010-12-05 00:00:00                    |
+----------------------------------------+
```
