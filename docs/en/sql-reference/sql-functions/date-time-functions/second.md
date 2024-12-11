---
displayed_sidebar: docs
---

# second

<<<<<<< HEAD
## Description
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

Returns the second part for a given date. The return value ranges from 0 to 59.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```Haskell
INT SECOND(DATETIME date)
```

## Examples

```Plain Text
MySQL > select second('2018-12-31 23:59:59');
+-----------------------------+
|second('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## keyword

SECOND
