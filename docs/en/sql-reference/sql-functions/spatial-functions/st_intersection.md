---
displayed_sidebar: docs
description: "Returns the planar overlap of two native GEOMETRY polygons."
---

# ST_Intersection

Computes the planar overlap of two native `GEOMETRY` values. Both inputs must be valid XY `POLYGON` or `MULTIPOLYGON` values with compatible CRS descriptors. The result preserves the input CRS; no reprojection is performed.

## Syntax

```SQL
ST_Intersection(GEOMETRY lhs, GEOMETRY rhs)
```

The result can be a point, line, polygon, multi-geometry, geometry collection, or empty geometry, depending on how boundaries meet. `NULL` produces `NULL`. Invalid topology, unsupported dimensions or input families, mismatched CRS descriptors, and `GEOGRAPHY` inputs are rejected.

The operation uses GEOS default floating precision on the input double coordinates. StarRocks does not request a snapping grid or repair invalid inputs; result boundaries can reflect floating-point rounding.

Each input and result WKB is limited to 64 MiB. A result with more than 1,000,000 coordinates is rejected; the native WKB codec also limits total geometry elements and nesting depth.

## Example

```SQL
SELECT ST_AsText(ST_Intersection(
    ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))', 'EPSG:3857')));
```

For these inputs, `ST_Area` of the result is `1` square CRS units.

## Keywords

ST_INTERSECTION,ST,GEOMETRY,POLYGON
