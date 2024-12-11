---
displayed_sidebar: docs
---

# weekofyear

<<<<<<< HEAD
## Description
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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
