---
displayed_sidebar: docs
description: "Jointly simplifies a window partition of native planar polygons while preserving coverage topology."
---

# ST_CoverageSimplify

Jointly simplifies an entire window partition of edge-matched native Cartesian XY polygons. Shared edges are simplified together, so adjacent rows retain the same boundary. Each output stays with its original row and CRS. This differs from the scalar [ST_SimplifyPreserveTopology](st_simplifypreservetopology.md), which processes each geometry independently.

## Syntax

```SQL
ST_CoverageSimplify(GEOMETRY geom, DOUBLE tolerance)
    OVER (PARTITION BY coverage_id)
ST_CoverageSimplify(GEOMETRY geom, DOUBLE tolerance, BOOLEAN simplify_boundary)
    OVER (PARTITION BY coverage_id)
```

`OVER ()` treats all input rows as one partition. The whole partition is processed across chunk boundaries. `ORDER BY` inside `OVER`, explicit window frames, DISTINCT, IGNORE NULLS, RESPECT NULLS and analytic execution hints are unsupported. An outer query `ORDER BY` is allowed. There are no scalar, array or ordinary aggregate overloads. Spill is unsupported: use `SET enable_spill = false`.

## Parameters and input

`tolerance` and `simplify_boundary` must be foldable plan constants. Tolerance must be finite and nonnegative, and its square must be representable as a finite DOUBLE. The two-argument form defaults to `simplify_boundary = true`; an explicit NULL does not use the default.

Tolerance is an area-based Visvalingam–Whyatt parameter related to the square root of effective triangle area, in input coordinate units. EPSG:4326 uses Cartesian degrees and EPSG:3857 uses projected metres, not geodesic ground distance. No reprojection, snapping or coordinate clamping is performed. This is not a Hausdorff or maximum-distance guarantee and does not promise unchanged area, maximal vertex reduction or identical PostGIS output.

With `true`, both shared and exterior boundaries may simplify. With `false`, only shared internal edges simplify; exterior boundaries, gaps and unfilled hole boundaries retain their original edges. A hole filled by another coverage member is a shared internal boundary.

Inputs must be POLYGON or MULTIPOLYGON with the same planar GEOMETRY CRS descriptor. Nonempty polygon interiors must not overlap, and shared edges must have matching vertices. Gaps are allowed (`gapWidth = 0`); mismatched subdivisions, T-junctions, duplicate nonempty polygons, invalid rings or holes are errors. No repair, union or automatic noding is performed. GEOGRAPHY, POINT, LINESTRING, GEOMETRYCOLLECTION and Z/M are unsupported. UNKNOWN/MIXED storage dimension metadata is accepted only when the actual computed values pass XY validation.

## Result and errors

Returns native XY WKB GEOMETRY with the input CRS and one output per input row. POLYGON/MULTIPOLYGON family, component order, holes, ring winding, contacts, junctions and shared boundaries are preserved. A single-member MULTIPOLYGON remains MULTIPOLYGON. Row mapping is positional; identical inputs are not deduplicated.

- NULL geometry returns NULL at its original position and is excluded from the coverage.
- Typed POLYGON/MULTIPOLYGON EMPTY preserves its family and position.
- NULL tolerance or explicit NULL boundary returns NULL for every row without running the coverage kernel. Type and plan-constant checks still apply.
- Zero tolerance validates inputs and coverage, then returns the original WKB bytes, including repeated closing coordinates.
- Invalid parameters are rejected even for all-NULL/all-EMPTY or empty input. Invalid coverage, malformed WKB, unsupported descriptors/dimensions and exceeded limits fail the partition before its output is published.

## Resource limits

The following positive mutable BE settings are captured when each partition begins:

| Setting | Default | Limit |
| --- | ---: | --- |
| `geo_coverage_max_rows_per_partition` | 10000 | All rows, including NULL and EMPTY. |
| `geo_coverage_max_vertices_per_partition` | 1000000 | Original WKB coordinate positions, including closures and repetitions. |
| `geo_coverage_max_input_bytes_per_partition` | 67108864 | Input WKB bytes, including typed EMPTY. |
| `geo_coverage_max_working_bytes_per_partition` | 268435456 | Retained input, models, indices, scratch, mapping and output capacities in bytes. |

Admission checks run before window accumulation. Shared native input/output backing is counted at its retained allocation size, so a small visible slice can retain a larger chunk. Query memory limits also apply across partitions. Exceeding a limit returns an error naming the setting, without truncation or partial partition results. A 100 million charged-work-unit bound can reject complex topology below the size limits. Cancellation checks run during accumulation, kernel loops and output, and around bounded library operations; no fixed cancellation latency or execution time is promised.

## Examples

```SQL
SET enable_spill = false;
WITH districts AS (
    SELECT 1 AS district_id, 7 AS coverage_id,
           'POLYGON ((0 0,0 8,4 8,4.1 6,3.8 4,4.2 2,4 0,0 0))' AS wkt
    UNION ALL
    SELECT 2, 7, 'POLYGON ((4 0,4.2 2,3.8 4,4.1 6,4 8,8 8,8 0,4 0))'
)
SELECT district_id,
       ST_AsText(ST_CoverageSimplify(ST_GeomFromText(wkt, 'EPSG:3857'), 1, false)
                 OVER (PARTITION BY coverage_id)) AS simplified
FROM districts ORDER BY district_id;
-- 1  POLYGON ((0 0, 0 8, 4 8, 4 0, 0 0))
-- 2  POLYGON ((4 0, 4 8, 8 8, 8 0, 4 0))

SELECT ST_AsText(ST_CoverageSimplify(
    ST_GeomFromText('MULTIPOLYGON EMPTY', 'EPSG:4326'), 1) OVER ());
-- MULTIPOLYGON EMPTY

SELECT ST_AsText(ST_CoverageSimplify(
    ST_GeomFromText('POLYGON EMPTY', 'EPSG:3857'), 0, NULL) OVER ());
-- NULL
```
