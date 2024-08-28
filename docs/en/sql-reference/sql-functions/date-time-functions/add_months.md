---
displayed_sidebar: docs
---

# add_months

## Description

Adds a specified number of months to a given date (DATE or DATETIME). The [months_add](./months_add.md) function provides similar functionalities.

The day component in the resulting month remains the same as that specified in `date`, unless the resulting month has fewer days than the day component of the given date, in which case the day will be the last day of the resulting month. For example, `select add_months('2022-01-31', 1);` returns `2022-02-28 00:00:00`. If the resulting month has more days than the day component of the given date, the result has the same day component as `date`. For example, `select add_months('2022-02-28', 1)` returns `2022-03-28 00:00:00`.

Difference with Oracle: In Oracle, if `date` is the last day of the month, then the result is the last day of the resulting month.

Returns NULL if an invalid date or a NULL argument is passed in.

## Syntax

```Haskell
ADD_MONTH(date, months)
```

## Parameters

- `date`: It must be a valid date or datetime expression.
- `months`: the months you want to add. It must be an integer. A  positive integer adds months to `date`. A negative integer subtracts months from `date`.

## Return value

Returns a DATETIME value. If the date does not exist, for example, `2020-02-30`, NULL is returned.

If the date is a DATE value, it will be converted into a DATETIME value.

## Examples

```Plain Text
> select add_months('2022-01-01', 2);
+-----------------------------+
| add_months('2022-01-01', 2) |
+-----------------------------+
| 2022-03-01 00:00:00         |
+-----------------------------+

> select add_months('2022-01-01', -5);
+------------------------------+
| add_months('2022-01-01', -5) |
+------------------------------+
| 2021-08-01 00:00:00          |
+------------------------------+

> select add_months('2022-01-31', 1);
+-----------------------------+
| add_months('2022-01-31', 1) |
+-----------------------------+
| 2022-02-28 00:00:00         |
+-----------------------------+

> select add_months('2022-01-31 17:01:02', -2);
+---------------------------------------+
| add_months('2022-01-31 17:01:02', -2) |
+---------------------------------------+
| 2021-11-30 17:01:02                   |
+---------------------------------------+

> select add_months('2022-02-28', 1);
+-----------------------------+
| add_months('2022-02-28', 1) |
+-----------------------------+
| 2022-03-28 00:00:00         |
+-----------------------------+
```
