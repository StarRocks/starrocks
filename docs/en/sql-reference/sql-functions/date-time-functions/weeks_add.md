---
displayed_sidebar: "English"
---

# weeks_add

## Description

Returns the value with the number of weeks added to date.

## Syntax

```Haskell
DATETIME weeks_add(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: the original date. It must be of the `DATETIME` type.

- `expr2`: the number of weeks. It must be of the `INT` type.

## Return value

returns `DATETIME`. 

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
