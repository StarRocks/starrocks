---
displayed_sidebar: docs
---

# weeks_sub

## Description

Subtracts a specified number of weeks from a datetime or date value.

## Syntax

```Haskell
DATETIME weeks_sub(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: the original date. It must be of the `DATETIME` type.

- `expr2`: the number of weeks. It must be of the `INT` type.

## Return value

Returns a `DATETIME` value.

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
