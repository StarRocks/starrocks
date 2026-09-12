---
displayed_sidebar: docs
description: "Converts a WKT (Well Known Text) to the corresponding memory geometry."
---

# ST_GeomFromText



Converts a two-dimensional OGC Well-Known Text (WKT) value to a native `GEOMETRY` value.

## Syntax

```Haskell
GEOMETRY ST_GeomFromText(VARCHAR wkt)
```

`ST_GeometryFromText` continues to return the legacy in-memory geography representation for compatibility.

The function supports `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION`, and their `EMPTY` forms. EWKT, SRID, and Z/M ordinates are not supported.

## Examples

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))'));
+-----------------------------------------------------------------------------------------------+
| st_astext(st_geomfromtext('GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))'))          |
+-----------------------------------------------------------------------------------------------+
| GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))                                       |
+-----------------------------------------------------------------------------------------------+
```

## keyword

ST_GEOMFROMTEXT,ST,GEOMFROMTEXT
