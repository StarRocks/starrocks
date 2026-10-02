---
displayed_sidebar: docs
description: "Subtracts one native planar polygon or multipolygon from another."
---

# ST_Difference

Returns the area of the first input that is outside the second input. Argument order matters. Holes and disconnected components are preserved.

## Syntax

```SQL
ST_Difference(GEOMETRY lhs, GEOMETRY rhs)
```

## Behavior and limits

Both arguments must be native XY `POLYGON` or `MULTIPOLYGON` values with compatible CRS descriptors. All four polygon/multipolygon combinations are supported. The return type is native `GEOMETRY` with the same CRS and XY coordinates: one area component is a `POLYGON`, multiple components are a `MULTIPOLYGON`, and an empty result is `POLYGON EMPTY`.

`NULL` propagates. `EMPTY` is an empty area, not `NULL`. Non-NULL inputs are checked for structural and topological validity, including holes and relationships between multipolygon components, before applying empty-input behavior. Invalid inputs produce errors; they are not repaired.

These are Cartesian operations even for EPSG:4326 (straight edges in degrees). EPSG:3857 uses projected coordinates in metres. Coordinates are never implicitly transformed. `GEOGRAPHY`, points, lines, collections, Z/M coordinates, incompatible CRS descriptors, aggregate/array forms and precision-grid arguments are unsupported.

Calculations use floating-point arithmetic without snapping or integer rescaling. WKB output uses double coordinates. Ring orientation, start vertex and component order can change. If rounding makes the output invalid, the function reports an error instead of repairing it.

Each input is limited to 256 KiB WKB and 5,000 coordinates, including ring-closing coordinates. For counts `n` and `m`, the function also requires `n*n + m*m + n*m <= 25,000,000`. Output and each symmetric-difference intermediate are limited to 5,000 coordinates; the intermediate pair also obeys the same work bound. Exceeding a limit produces an error, without truncation. Query memory limits apply to the input models and temporary allocations. Cancellation is checked between rows and around overlay calls; a single Boost call has no cancellation callback. These bounds are not a fixed peak-memory or wall-clock guarantee.

## Examples

```SQL
SELECT ST_Area(ST_Difference(
    ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((2 0, 6 0, 6 4, 2 4, 2 0))', 'EPSG:3857')));
-- 8

SELECT ST_AsText(ST_Difference(
    ST_GeomFromText('POLYGON EMPTY', 'EPSG:3857'),
    ST_GeomFromText('POLYGON EMPTY', 'EPSG:3857')));
-- POLYGON EMPTY
```
