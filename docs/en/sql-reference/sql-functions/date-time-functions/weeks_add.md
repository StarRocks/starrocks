---
displayed_sidebar: docs
---

# weeks_add

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Returns the value with the number of weeks added to date.

## Syntax

```Haskell
DATETIME weeks_add(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: the original date. It must be of the `DATETIME` type.

- `expr2`: the number of weeks. It must be of the `INT` type.

## Return value

<<<<<<< HEAD
returns `DATETIME`.
=======
returns `DATETIME`. 
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

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
