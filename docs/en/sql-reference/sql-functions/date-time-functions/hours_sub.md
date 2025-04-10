---
displayed_sidebar: docs
---

# hours_sub

## Description

Reduces the specified date and time by a specified number of hours.

## Syntax

```Haskell
DATETIME hours_sub(DATETIME|DATE date, INT hours);
```

## Parameters

* `date`: It must be a valid DATE or DATETIME expression.

* `hours`: The number of hours reduced. The supported data type is INT.

## Return value

Returns a DATETIME value. If the date does not exist, for example, `2022-21-01`, or if the date is not a DATE or DATETIME value, NULL is returned.

## Examples

```Plain Text
select hours_sub('2022-01-01 01:01:01', 1);
+-------------------------------------+
| hours_sub('2022-01-01 01:01:01', 1) |
+-------------------------------------+
| 2022-01-01 00:01:01                 |
+-------------------------------------+

select hours_sub('2022-01-01 01:01:01', -1);
+--------------------------------------+
| hours_sub('2022-01-01 01:01:01', -1) |
+--------------------------------------+
| 2022-01-01 02:01:01                  |
+--------------------------------------+

select hours_sub('2022-01-01', 1);
+----------------------------+
| hours_sub('2022-01-01', 1) |
+----------------------------+
| 2021-12-31 23:00:00        |
+----------------------------+

select hours_sub('2022-01-01', -1);
+-----------------------------+
| hours_sub('2022-01-01', -1) |
+-----------------------------+
| 2022-01-01 01:00:00         |
+-----------------------------+

Error case:
select hours_sub('2022-21-01', -1);
+--------------------------------------+
| hours_sub('2022-21-01', -1) |
+--------------------------------------+
| NULL                                 |
+--------------------------------------+
```
