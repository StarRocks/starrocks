---
displayed_sidebar: docs
---

# weekofyear

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Returns the week number for a given date within a year.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT WEEKOFYEAR(DATETIME date)
```

## Examples

```Plain Text
MySQL > select weekofyear('2008-02-20 00:00:00');
+-----------------------------------+
| weekofyear('2008-02-20 00:00:00') |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```

## keyword

WEEKOFYEAR
