# last_day

## Description

Returns the last day of an input DATE or DATETIME expression based on the specified date part.

This function is supported from v3.1.

## Syntax

```SQL
DATE last_day(DATETIME|DATE date_expr[, VARCHAR unit])
```

## Parameters

- `date_expr`: a DATE or DATETIME expression, required.

- `unit`: the date part, optional. Valid values include `month`, `quarter`, and `year`, default to `month`. If `unit` is invalid, an error is returned.

## Return value

Returns a DATE value.

## Examples

```Plain
MySQL > select last_day('2023-05-10', 'month');
+----------------------------------+
| last_day('2023-05-10', 'month') |
+----------------------------------+
| 2023-05-31                       |
+----------------------------------+

MySQL > select last_day('2023-05-10');
+------------------------+
| last_day('2023-05-10') |
+------------------------+
| 2023-05-31             |
+------------------------+

MySQL > select last_day('2023-05-10', 'quarter');
+-------------------------------+
| last_day('2023-05-10', 'quarter') |
+-------------------------------+
| 2023-06-30                    |
+-------------------------------+

MySQL > select last_day('2023-05-10', 'year');
+---------------------------------------+
| last_day('2023-05-10', 'year') |
+---------------------------------------+
| 2023-12-31                            |
+---------------------------------------+
```

## keyword

LAST_DAY, LAST
