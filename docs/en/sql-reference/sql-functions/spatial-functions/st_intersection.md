---
displayed_sidebar: docs
description: "Returns the shared area, edges, and isolated contact points of two native planar polygons."
---

# ST_Intersection

Returns the set shared by two polygons or multipolygons, including boundary contacts.

## Syntax

```SQL
ST_Intersection(GEOMETRY lhs, GEOMETRY rhs)
```

## Behavior and limits

Both arguments must be native XY `POLYGON` or `MULTIPOLYGON` values with compatible CRS descriptors. All four polygon/multipolygon input combinations are supported. The result is native `GEOMETRY` with the same CRS and XY coordinates.

Area parts return `POLYGON` or `MULTIPOLYGON`. Shared edges return `LINESTRING` or `MULTILINESTRING`; isolated contact points return `POINT` or `MULTIPOINT`. A result containing more than one of these dimensions is a `GEOMETRYCOLLECTION`. Edges and points already covered by the area, and points covered by returned lines, are not repeated as separate parts. An empty intersection returns `POLYGON EMPTY`.

`NULL` propagates. Non-NULL operands are checked for structural and topological validity before empty-input behavior. Invalid input is an error and is not repaired. `GEOGRAPHY`, point/line/collection inputs, Z/M coordinates, incompatible CRS descriptors, and precision-grid arguments are unsupported.

These are Cartesian operations even for EPSG:4326 (straight edges in degrees); EPSG:3857 uses metres. Coordinates are never implicitly transformed. Input preparation and precision follow the [polygon overlay contract](st_union.md): local floating-point arithmetic without integer rescaling or snapping, with double coordinates in WKB. Polygon topology is checked after operand translation and output rounding. Collapsed line segments, nonfinite output, or distinct isolated points rounding to the same WKB point cause an error. Ring orientation, start vertex, component order, and line segmentation can change.

Each input is limited to 256 KiB WKB and 5,000 coordinates, including ring-closing coordinates. For counts `n` and `m`, `n*n + m*m + n*m` must not exceed 25,000,000. The total output across all dimensions is limited to 5,000 coordinates. Exceeding a limit produces an error without truncation. Query memory limits apply to preparation and temporary allocations. Cancellation is checked between rows and around the area, boundary-intersection, and clipping calls; a single Boost call has no cancellation callback. These bounds are not a fixed peak-memory or wall-clock guarantee.

## Examples

```SQL
SELECT ST_Area(ST_Intersection(
    ST_GeomFromText('POLYGON ((0 0,4 0,4 4,0 4,0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((2 0,6 0,6 4,2 4,2 0))', 'EPSG:3857')));
-- 8

SELECT ST_GeometryType(ST_Intersection(
    ST_GeomFromText('POLYGON ((0 0,4 0,4 4,0 4,0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((4 0,8 0,8 4,4 4,4 0))', 'EPSG:3857')));
-- ST_LineString
```
