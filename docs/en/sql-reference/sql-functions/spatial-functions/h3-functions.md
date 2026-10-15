---
displayed_sidebar: docs
description: "Indexes native GEOGRAPHY with the seven H3 cell and polygon functions."
---

# H3 functions

These functions use H3 4.5.0 cells with native XY CRS84 `GEOGRAPHY` (longitude, latitude in degrees). The public cell index is a positive signed `BIGINT`, including values above 2^53. Keep it in a lossless 64-bit client type or transmit it as a decimal string; JavaScript `Number` cannot represent every cell exactly. No `GEOMETRY` or implicit CRS conversion is supported.

| Function | Return | Rule |
| --- | --- | --- |
| `H3_FromGeo(geography, resolution INT)` | `BIGINT` | Cell containing a nonempty `POINT`; resolution 0вЂ“15. `POINT EMPTY` gives `NULL`. |
| `H3_GridDisk(cell BIGINT, k INT)` | `ARRAY<BIGINT>` | Cells within `k` grid steps, including the source; `k >= 0`, `k = 0` gives `[cell]`. |
| `H3_ToParent(cell BIGINT, resolution INT)` | `BIGINT` | Parent at a resolution from 0 through the cell's resolution. |
| `H3_ToChildren(cell BIGINT, resolution INT)` | `ARRAY<BIGINT>` | Descendants at a resolution from the cell's resolution through 15. |
| `H3_Resolution(cell BIGINT)` | `INT` | Resolution of a valid cell. |
| `H3_ToBoundary(cell BIGINT)` | `GEOGRAPHY` | Closed native CRS84 polygon with spherical edges and every vertex returned by H3. |
| `H3_PolygonToCells(geography, resolution INT)` | `ARRAY<BIGINT>` | Standard H3 center-fill of valid `POLYGON`/`MULTIPOLYGON`, including holes; resolution 0вЂ“15. Empty polygon gives `[]`. |

Any `NULL` argument gives `NULL`. Array elements are unique valid cell indexes, but their order is unspecified; use `array_sort` for stable display. An invalid cell (including 0, negative values, edge or vertex indexes), resolution, topology, CRS, dimension, or geometry family raises an error. Invalid numeric arguments remain errors even for an `EMPTY` geography.

`H3_PolygonToCells` selects cells using H3's standard center-fill (`flags = 0`), rather than exact spherical `ST_Contains`/`ST_Covers`. A nonempty polygon may return `[]`; the result is neither conservative coverage nor a safe replacement for exact spatial predicates. H3's longitude/latitude fill model has limitations near the antimeridian, poles, and for large polygons. A center on a boundary follows H3 4.5.0 rules. The array can be used as an approximate index, followed by the appropriate exact predicate when correctness needs it.

## Examples

```sql
SELECT H3_FromGeo(ST_GeogFromText('POINT (0 0)'), 3);
-- 592035265791393791
SELECT H3_Resolution(592035265791393791);
-- 3
SELECT H3_ToParent(592035265791393791, 2);
SELECT array_sort(H3_GridDisk(592035265791393791, 1));
SELECT array_sort(H3_ToChildren(592035265791393791, 4));
SELECT ST_AsText(H3_ToBoundary(592035265791393791));
SELECT array_sort(H3_PolygonToCells(
  ST_GeogFromText('POLYGON ((-5 -5, 5 -5, 5 5, -5 5, -5 -5))'), 3));
```
