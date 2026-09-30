---
displayed_sidebar: docs
description: "Returns the planar area in the left input outside the right input of two native GEOMETRY polygons."
---

# ST_Difference

Computes the planar area in the left input outside the right input of two native `GEOMETRY` values. Both inputs must be valid XY `POLYGON` or `MULTIPOLYGON` values with compatible CRS descriptors. The result preserves the input CRS; no reprojection is performed.

## Syntax

```SQL
ST_Difference(GEOMETRY lhs, GEOMETRY rhs)
```

The result can be a point, line, polygon, multi-geometry, geometry collection, or empty geometry, depending on how boundaries meet. `NULL` produces `NULL`. Invalid topology, unsupported dimensions or input families, mismatched CRS descriptors, and `GEOGRAPHY` inputs are rejected.

The operation uses GEOS default floating precision on the input double coordinates. StarRocks does not request a snapping grid or repair invalid inputs; result boundaries can reflect floating-point rounding.

## Example

```SQL
SELECT ST_AsText(ST_Difference(
    ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))', 'EPSG:3857')));
```

For these inputs, `ST_Area` of the result is `3` square CRS units.

## Keywords

ST_DIFFERENCE,ST,GEOMETRY,POLYGON
