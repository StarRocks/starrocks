# ST_Y

## description

### Syntax

```Haskell
DOUBLE ST_Y(POINT point)
```

If point is of a valid Point type, return the corresponding Y-coordinate value.

## example

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
