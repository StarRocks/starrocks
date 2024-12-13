---
displayed_sidebar: docs
---

# ST_Point

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Returns the corresponding Point with the given X-coordinate and Y-coordinate. At the moment this value only makes sense on a spherical set. X/Y corresponds to longitude/latitude.

> **Caution**
>
> If you directly select ST_Point(), it may get stuck.

## Syntax

```Haskell
POINT ST_Point(DOUBLE x, DOUBLE y)
```

## Examples

```Plain Text
MySQL > SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```

## keyword

ST_POINT,ST,POINT
