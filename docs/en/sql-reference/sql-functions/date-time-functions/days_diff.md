---
displayed_sidebar: docs
---

# days_diff

## Description

Returns the day difference between two date expressions (`expr1` − `expr2`), accurate to the day.

Difference between days_diff and [datediff](./datediff.md):

|Function|Behavior|Example|
|---|---|---|
|days_diff| The calculation is accurate to the second and rounded down to the nearest integer.|days_diff between '2020-12-25 23:00:00' and '2020-12-24 23:00:01' is 0.|
|datediff| The calculation is accurate to the day. |datediff between '2020-12-25 23:00:00' and '2020-12-24 23:00:01' is 1.|

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
