---
displayed_sidebar: docs
---

# ST_Y

<<<<<<< HEAD
## Description
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

If point is of a valid Point type, return the corresponding Y-coordinate value.

## Syntax

```Haskell
DOUBLE ST_Y(POINT point)
```

## Examples

```Plain Text
MySQL > SELECT ST_Y(ST_Point(24.7, 56.7));
+----------------------------+
| st_y(st_point(24.7, 56.7)) |
+----------------------------+
|                       56.7 |
+----------------------------+
```

## keyword

ST_Y,ST,Y
