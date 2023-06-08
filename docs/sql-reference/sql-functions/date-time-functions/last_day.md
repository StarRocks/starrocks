# last_day

## Description

Returns the last day of the specified date part for a date or datetime.

## Syntax

```SQL
DATE last_day(DATETIME|DATE expr1, [VARCHAR expr2])
```

## Parameters

- `expr1`: is a date or datetime expression.

- `expr2`: is an optional parameter. can be `month`, `quarter`,`year`, default to `month`.

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
