---
displayed_sidebar: docs
description: "Simplifies native XY geometry while preserving topology and its CRS."
---

# ST_SimplifyPreserveTopology

Reduces vertices of native Cartesian XY `GEOMETRY` while preserving validity, dimension, components, rings, hole ownership, existing contacts and disjointness. Unsafe deletions leave the original section in place. This is a scalar operation on one geometry, not coverage simplification across rows.

## Syntax

```SQL
ST_SimplifyPreserveTopology(GEOMETRY geometry, DOUBLE tolerance)
```

Tolerance may be constant or vary by row. It must be finite and nonnegative, in input coordinate units. There is no third argument.

## Input and result

Supports POINT, LINESTRING, POLYGON, their MULTI families, and nested GEOMETRYCOLLECTION. The result preserves the input family, component order, collection tree, typed EMPTY children, CRS and planar XY descriptor. A single-member MULTIPOLYGON remains MULTIPOLYGON. Points and duplicate point members stay unchanged. Lines retain endpoints; rings retain closure and winding. The result uses only original vertices in their original path order.

`NULL` in either argument returns `NULL`. Zero tolerance returns the original WKB after structural, dimensional and topology validation. EMPTY remains the same typed EMPTY; non-NULL EMPTY still requires valid tolerance. Negative, NaN or infinite tolerance, malformed WKB, invalid polygon topology, degenerate lines/rings and exceeded limits produce errors. Simplification does not repair invalid input or discard components to obtain a valid result.

GEOGRAPHY, Z/M and unknown or mixed dimensions are unsupported. Collection support here does not extend other GEO functions. The signature is not an aggregate, window or array operation.

## Units and error bound

EPSG:4326 uses Cartesian degrees: tolerance 1000 means **1000 degrees**. EPSG:3857 uses projected metres, not geodesic ground distance. No transform, wrapping, clamping or spherical calculation is applied.

For line paths and polygon boundaries, the two-sided Hausdorff distance between input and result is at most tolerance. Each replacement segment is checked against its entire original subchain, including previously deleted vertices, so error does not accumulate. Contact and distance decisions use exact dyadic comparisons of the stored finite double coordinates; no hidden epsilon, snapping or precision grid changes the input. The result does not promise unchanged area, a distance bound between filled polygon interiors, maximal reduction, or identical vertices to PostGIS/JTS. Shared boundaries between separate rows are not constrained.

## Resource limits

Per input: 256 KiB WKB, 5,000 coordinate positions including closures, 1,024 geometry nodes plus rings, and nesting depth 32. Preparation and each evaluation are subject to a 20 million charged-work-unit limit. Exact integer size, active edges, indices and query results are bounded; a work-limit error can occur even below the input size limits. Query memory limits cover prepared models and temporary allocations. Cancellation and memory checks run between rows, before bounded allocation groups and within charged loops. These limits do not promise a fixed peak-memory amount or execution time.

## Examples

```SQL
SELECT ST_AsText(ST_SimplifyPreserveTopology(
    ST_GeomFromText('LINESTRING (0 0,1 0,2 0)', 'EPSG:3857'), 0.1));
-- LINESTRING (0 0, 2 0)

SELECT ST_AsText(ST_SimplifyPreserveTopology(
    ST_GeomFromText('MULTIPOLYGON EMPTY', 'EPSG:4326'), 1));
-- MULTIPOLYGON EMPTY

SELECT ST_SRID(ST_SimplifyPreserveTopology(
    ST_GeomFromText('POINT (1 2)', 'EPSG:4326'), 0));
-- 4326

SELECT ST_AsText(ST_SimplifyPreserveTopology(
    ST_GeomFromText('POINT (1 2)', 'EPSG:3857'), NULL));
-- NULL
```
