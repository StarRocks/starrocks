# next_day

## Description

Returns the date of the first specified day of week (DOW) that occurs after the input date (DATE or DATETIME).

This function is supported from v3.1. It is the opposite of [previous_day](./previous_day.md).

## Syntax

```SQL
DATE next_day(DATETIME|DATE date_expr, VARCHAR dow)
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

Returns NULL if an invalid date or a NULL argument is passed in.

## Examples

```Plain
-- Return the date of the next Monday that occurred after 2023-04-06. 2023-04-06 is Thursday and the date of the next Monday is 2023-04-10.

MySQL > select next_day('2023-04-06', 'Monday');
+----------------------------------+
| next_day('2023-04-06', 'Monday') |
+----------------------------------+
| 2023-04-10                       |
+----------------------------------+

MySQL > select next_day('2023-04-06', 'Tue');
+-------------------------------+
| next_day('2023-04-06', 'Tue') |
+-------------------------------+
| 2023-04-11                    |
+-------------------------------+

MySQL > select next_day('2023-04-06 20:13:14', 'Fr');
+---------------------------------------+
| next_day('2023-04-06 20:13:14', 'Fr') |
+---------------------------------------+
| 2023-04-07                            |
+---------------------------------------+
```

## keyword

NEXT_DAY, NEXT
