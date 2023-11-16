---
displayed_sidebar: "English"
---

# months_sub

## Description

Subtracts specified months from the date.

## Syntax

```Haskell
DATETIME months_sub(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: the specified date. It must be of the DATETIME or DATE type.

- `expr2`: the month to subtract. It must be INT type.

## Return value

Returns a DATETIME value.

## Examples

```Plain Text
select months_sub('2022-02-28 15:04:05', 1);
+--------------------------------------+
| months_sub('2022-02-28 15:04:05', 1) |
+--------------------------------------+
| 2022-01-28 15:04:05                  |
+--------------------------------------+

select months_sub('2022-02-28', 1);
+-----------------------------+
| months_sub('2022-02-28', 1) |
+-----------------------------+
| 2022-01-28 00:00:00         |
+-----------------------------+
