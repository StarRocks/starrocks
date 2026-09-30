---
displayed_sidebar: docs
description: "Returns the planar combined area of two native GEOMETRY polygons."
---

# ST_Union

Computes the planar combined area of two native `GEOMETRY` values. Both inputs must be valid XY `POLYGON` or `MULTIPOLYGON` values with compatible CRS descriptors. The result preserves the input CRS; no reprojection is performed.

This is a binary scalar function; it does not aggregate geometries across rows.

## Syntax

```SQL
ST_Union(GEOMETRY lhs, GEOMETRY rhs)
```

The result can be a point, line, polygon, multi-geometry, geometry collection, or empty geometry, depending on how boundaries meet. `NULL` produces `NULL`. Invalid topology, unsupported dimensions or input families, mismatched CRS descriptors, and `GEOGRAPHY` inputs are rejected.

The operation uses GEOS default floating precision on the input double coordinates. StarRocks does not request a snapping grid or repair invalid inputs; result boundaries can reflect floating-point rounding.

## Example

```SQL
SELECT ST_AsText(ST_Union(
    ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))', 'EPSG:3857')));
```

For these inputs, `ST_Area` of the result is `7` square CRS units.

## Keywords

ST_UNION,ST,GEOMETRY,POLYGON
