# previous_day

## Description

Returns the date of the first specified DOW(day of week) that occurs before the input date (DATE, DATETIME).

Returns NULL if an invalid date or a NULL argument is passed in.

## Syntax

```SQL
DATE previous_day(DATETIME|DATE expr1, VARCHAR expr2)
```

## Parameters

- `expr1`: It must be a valid date or datetime expression.

- `expr2`: The day of week. Validvalues include a number of abbreviations:

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

## Examples

```Plain
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

PREVIOUS_DAY, PREVIOUS
