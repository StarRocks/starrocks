# ST_X

## description

### Syntax

```Haskell
DOUBLE ST_X(POINT point)
```

If point is of a valid Point type, return the corresponding X-coordinate value.

## example

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
