---
displayed_sidebar: "English"
---

# weeks_sub

## Description

Returns the value with the number of weeks minused to date.

## Syntax

```Haskell
DATETIME weeks_sub(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: the original date. It must be of the `DATETIME` type.

- `expr2`: the number of weeks. It must be of the `INT` type.

## Return value

returns `DATETIME`. 

`NULL` is returned if the date does not exist.

## Examples

```Plain
select weeks_sub('2022-12-22',2);
+----------------------------+
| weeks_sub('2022-12-22', 2) |
+----------------------------+
|        2022-12-08 00:00:00 |
+----------------------------+
```
