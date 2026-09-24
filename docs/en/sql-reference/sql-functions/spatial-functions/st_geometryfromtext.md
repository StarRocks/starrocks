---
displayed_sidebar: docs
description: "Constructs native GEOMETRY from WKT with an explicit CRS, or uses the legacy one-argument format."
---

# ST_GeomFromText, ST_GeometryFromText



`ST_GeomFromText(wkt, crs)` constructs a native planar `GEOMETRY` value from two-dimensional OGC Well-Known Text (WKT). The CRS is required and becomes part of the native type descriptor.

The existing one-argument `ST_GeomFromText` and `ST_GeometryFromText` signatures keep returning the legacy `VARCHAR` representation.

## Syntax

```Haskell
GEOMETRY ST_GeomFromText(VARCHAR wkt, VARCHAR crs)
VARCHAR ST_GeomFromText(VARCHAR wkt)
VARCHAR ST_GeometryFromText(VARCHAR wkt)
```

For the native signature, `crs` must be a non-empty string literal. There is no default CRS and no implicit coordinate conversion or reprojection. The function supports all seven 2D OGC geometry families, `EMPTY`, and empty children. Invalid WKT returns `NULL`; a `NULL` WKT returns `NULL`.

## Examples

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('LINESTRING (1 1, 2 2)', 'EPSG:3857'));
+----------------------------------------------------------------------------+
| st_astext(st_geomfromtext('LINESTRING (1 1, 2 2)', 'EPSG:3857'))           |
+----------------------------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                                      |
+----------------------------------------------------------------------------+
```

## keyword

ST_GEOMETRYFROMTEXT, ST_GEOMFROMTEXT, GEOMETRY, WKT, CRS
