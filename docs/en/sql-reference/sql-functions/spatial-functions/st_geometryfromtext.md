# ST_GeometryFromText,ST_GeomFromText

## description

### Syntax

```Haskell
GEOMETRY ST_GeometryFromText(VARCHAR wkt)
```

Convert a WKT (Well Known Text) to the corresponding memory geometry.

## example

```Plain Text
MySQL > SELECT ST_AsText(ST_GeometryFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                   |
+---------------------------------------------------------+
```

## keyword

ST_GEOMETRYFROMTEXT,ST_GEOMFROMTEXT,ST,GEOMETRYFROMTEXT,GEOMFROMTEXT
