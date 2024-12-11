---
displayed_sidebar: docs
---

# years_sub

<<<<<<< HEAD
## Description
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

Subtracts the specified number of years from the specified datetime or date.

## Syntax

```Haskell
DATETIME YEARS_SUB(DATETIME date, INT years)
```

## Parameters

<<<<<<< HEAD
- `date`: The original date time, of type DATETIME or DATE.

- `years`: The number of years to subtract. The value can be negative, but date year minus years can't exceed 10000. For example, if the year of date is 2022, then years can't be less than -7979. At the same time, the years cannot exceed the year value of date, for example, if the year value of date is 2022, then years can't be greater than 2022.
=======
`date`: The original date time, of type DATETIME or DATE.

`years`: The number of years to subtract. The value can be negative, but date year minus years can't exceed 10000. For example, if the year of date is 2022, then years can't be less than -7979. At the same time, the years cannot exceed the year value of date, for example, if the year value of date is 2022, then years can't be greater than 2022.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

## Return value

The return value type is the same as the parameter `date`. Returns NULL if the result year is out of range [0, 9999].

## Examples

```Plain Text
select years_sub("2022-12-20 15:50:21", 2);
+-------------------------------------+
| years_sub('2022-12-20 15:50:21', 2) |
+-------------------------------------+
| 2020-12-20 15:50:21                 |
+-------------------------------------+
```
