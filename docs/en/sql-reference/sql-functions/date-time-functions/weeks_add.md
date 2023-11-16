---
displayed_sidebar: "English"
---

# weeks_add

## Description

Adds a specified number of weeks to a date.

## Syntax

```Haskell
DATETIME weeks_add(DATETIME|DATE expr1, INT expr2);
```

## Parameters

- `expr1`: the original date. It must be of the DATETIME or DATE type.

- `expr2`: the number of weeks to add. It must be of the `INT` type.

## Return value

Returns a DATETIME value. 

`NULL` is returned if the date does not exist.

## Examples

```Plain
select weeks_add('2022-12-20',2);
+----------------------------+
| weeks_add('2022-12-20', 2) |
+----------------------------+
|        2023-01-03 00:00:00 |
+----------------------------+
```
