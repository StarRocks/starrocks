# add_months

## Description

Adds an integer months to a given date (DATE, DATETIME). The integer can be positive or negative.

The resulting day component remains the same as that specified in `date`, unless the resulting month
has fewer days than the day component of the given date, in which case the day will be the last day of
the resulting month.

Returns NULL if an invalid date or a NULL argument is passed in.

## Syntax

```SQL	
ADD_MONTH(date, months)
```

## Parameters

- `date`: It must be a valid date or datetime expression.
- `months`: the months you want to add. It must be of the INT type.

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

> select add_months('2022-01-31', 2);
+-----------------------------+
| add_months('2022-01-31', 2) |
+-----------------------------+
| 2022-03-31 00:00:00         |
+-----------------------------+

> select add_months('2022-01-31 17:01:02', 2);
+--------------------------------------+
| add_months('2022-01-31 17:01:02', 2) |
+--------------------------------------+
| 2022-03-31 17:01:02                  |
+--------------------------------------+

> select add_months('2022-01-31 17:01:02', -2);
+---------------------------------------+
| add_months('2022-01-31 17:01:02', -2) |
+---------------------------------------+
| 2021-11-30 17:01:02                   |
+---------------------------------------+
```
