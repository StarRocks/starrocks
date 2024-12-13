---
displayed_sidebar: docs
---

# ST_GeometryFromText,ST_GeomFromText

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

Converts a WKT (Well Known Text) to the corresponding memory geometry.

## Syntax

```Haskell
GEOMETRY ST_GeometryFromText(VARCHAR wkt)
```

## Examples

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
