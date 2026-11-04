---
displayed_sidebar: docs
description: "Returns the dimension-weighted centroid of a native GEO value."
---

# ST_Centroid

Returns a `POINT` centroid with the same native logical type and descriptor as the input. A centroid is a weighted geometric center and is not necessarily inside a polygon.

## Syntax

```SQL
ST_Centroid(GEOGRAPHY value)
ST_Centroid(GEOMETRY value)
```

Polygon components are weighted by area, line components by length, and point components equally. Only the highest non-empty dimension present contributes: polygons take precedence over lines, and lines take precedence over points. This rule is applied recursively to `MULTI*` values and `GEOMETRYCOLLECTION` values. Polygon holes subtract their area and moment.

`GEOGRAPHY` computes the centroid on the unit sphere and returns longitude/latitude in OGC:CRS84 order. `GEOMETRY` uses planar coordinates. The result preserves the input CRS and logical kind; no reprojection occurs.

`NULL` produces `NULL`; an `EMPTY` value produces `POINT EMPTY`. A zero resultant spherical vector, or an input with no non-empty component, produces `POINT EMPTY`. Malformed WKB, unsupported dimensions or descriptors, and topologically invalid inputs produce a controlled error.

## Examples

```SQL
SELECT ST_AsText(ST_Centroid(ST_GeomFromText(
    'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 'EPSG:3857')));
-- POINT (2 2)

SELECT ST_AsText(ST_Centroid(ST_GeomFromText(
    'GEOMETRYCOLLECTION (POINT (100 100), LINESTRING (0 0, 4 0))',
    'EPSG:3857')));
-- POINT (2 0): the line has the highest dimension
```

## Keywords

ST_CENTROID,ST,CENTROID,GEOGRAPHY,GEOMETRY
