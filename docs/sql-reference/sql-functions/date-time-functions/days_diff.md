# days_diff

## Description

Returns the day difference between two date expressions (`expr1` − `expr2`), accurate to the day.

## Syntax

```Haskell
BIGINT days_diff(DATETIME expr1,DATETIME expr2);
```

## Parameters

- `expr1`: the end time. It must be of the DATETIME or DATE type.

- `expr2`: the start time. It must be of the DATETIME or DATE type.

## Return value

Returns a BIGINT value.

NULL is returned if the date does not exist, for example, 2022-02-29. days_diff() takes the hour, minute, and second parts into calculation. If the difference is less than 1 day, 0 is returned.

## Examples

```Plain
select days_diff('2010-11-30 23:00:00', '2010-11-29 23:00:00')
+---------------------------------------------------------+
| days_diff('2010-11-30 23:00:00', '2010-11-29 23:00:00') |
+---------------------------------------------------------+
| 1                                                       |
+---------------------------------------------------------+

select days_diff('2010-11-30 23:00:00', '2010-11-29 23:10:00')
+---------------------------------------------------------+
| days_diff('2010-11-30 23:00:00', '2010-11-29 23:10:00') |
+---------------------------------------------------------+
| 0                                                       |
+---------------------------------------------------------+
```
