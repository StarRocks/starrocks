---
displayed_sidebar: "English"
---

# days_add

## Description

Adds a specified number of days to a given date or date time.

## Syntax

```Haskell
DATETIME days_add(DATETIME|DATE date, INT n);
```

## Parameters

`date`: a DATE or DATETIME expression.

`n`: the number of days to add.

## Return value

Returns a DATETIME value.

If any input parameter is NULL or invalid, NULL is returned.

If the output date exceeds the range of [0000-01-01 00:00:00, 9999-12-31 00:00:00], NULL is returned.

## Examples

```Plain Text
select days_add('2023-10-31 23:59:59', 1);
+------------------------------------+
| days_add('2023-10-31 23:59:59', 1) |
+------------------------------------+
| 2023-11-01 23:59:59                |
+------------------------------------+

select days_add('2023-10-31 23:59:59', 1000);
+---------------------------------------+
| days_add('2023-10-31 23:59:59', 1000) |
+---------------------------------------+
| 2026-07-27 23:59:59                   |
+---------------------------------------+

select days_add('2023-10-31', 1);
+---------------------------+
| days_add('2023-10-31', 1) |
+---------------------------+
| 2023-11-01 00:00:00       |
+---------------------------+
```

## Keywords

DAY,day
