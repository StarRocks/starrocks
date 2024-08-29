---
displayed_sidebar: docs
---

# previous_day

## Description

Returns the date of the first specified day of week (DOW) that occurs before the input date (DATE  or DATETIME). For example, `previous_day('2023-04-06', 'Monday')` returns the date of the previous Monday that occurred before '2023-04-06'.

This function is supported from v3.1. It is the opposite of [next_day](./next_day.md).

## Syntax

```SQL
DATE previous_day(DATETIME|DATE date_expr, VARCHAR dow)
```

## Parameters

- `date_expr`: the input date. It must be a valid DATE or DATETIME expression.
- `dow`: the day of week. Valid values include a number of abbreviations which are case-sensitive:

  | DOW_FULL  | DOW_2 | DOW_3 |
  | --------- | ----- |:-----:|
  | Sunday    | Su    | Sun   |
  | Monday    | Mo    | Mon   |
  | Tuesday   | Tu    | Tue   |
  | Wednesday | We    | Wed   |
  | Thursday  | Th    | Thu   |
  | Friday    | Fr    | Fri   |
  | Saturday  | Sa    | Sat   |

## Return value

Returns a DATE value.

Any invalid `dow` will cause an error. `dow` is case-sensitive.

NULL is returned if an invalid date or a NULL argument is passed in.

## Examples

```Plain
-- Return the date of the previous Monday that occurred before 2023-04-06. 2023-04-06 is Thursday and the date of the previous Monday is 2023-04-03.

MySQL > select previous_day('2023-04-06', 'Monday');
+--------------------------------------+
| previous_day('2023-04-06', 'Monday') |
+--------------------------------------+
| 2023-04-03                           |
+--------------------------------------+

MySQL > select previous_day('2023-04-06', 'Tue');
+-----------------------------------+
| previous_day('2023-04-06', 'Tue') |
+-----------------------------------+
| 2023-04-04                        |
+-----------------------------------+

MySQL > select previous_day('2023-04-06 20:13:14', 'Fr');
+-------------------------------------------+
| previous_day('2023-04-06 20:13:14', 'Fr') |
+-------------------------------------------+
| 2023-03-31                                |
+-------------------------------------------+
```

## keyword

PREVIOUS_DAY, PREVIOUS, previousday
