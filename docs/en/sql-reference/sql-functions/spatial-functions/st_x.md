---
displayed_sidebar: docs
---

# ST_X

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

If point is of a valid Point type, return the corresponding X-coordinate value.

## Syntax

```Haskell
DOUBLE ST_X(POINT point)
```

## Examples

```Plain Text
MySQL > SELECT ST_X(ST_Point(24.7, 56.7));
+----------------------------+
| st_x(st_point(24.7, 56.7)) |
+----------------------------+
|                       24.7 |
+----------------------------+
```

## keyword

ST_X,ST,X
