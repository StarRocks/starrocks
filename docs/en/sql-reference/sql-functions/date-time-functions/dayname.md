---
displayed_sidebar: docs
---

# dayname

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Returns the day corresponding to a date.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
VARCHAR DAYNAME(date)
```

## Examples

```Plain Text
MySQL > select dayname('2007-02-03 00:00:00');
+--------------------------------+
| dayname('2007-02-03 00:00:00') |
+--------------------------------+
| Saturday                       |
+--------------------------------+
```

## keyword

DAYNAME
