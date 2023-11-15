# ST_Point

## description

### Syntax

```Haskell
POINT ST_Point(DOUBLE x, DOUBLE y)
```

The st_point returns the corresponding Point with the given X-coordinate and Y-coordinate. At the moment this value only makes sense on a spherical set. X/Y corresponds to longitude/latitude.

> Caution!!! If you directly select ST_Point(), it  may get stuck.

## example

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
