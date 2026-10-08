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

H3 4.5.0 does not expose cancellation inside a single `polygonToCells` call. StarRocks checks cancellation around that call and between rows; the admission limits below bound work but do not guarantee a fixed cancellation latency.

The `WITH_H3` BE build option is enabled by default. A BE built with `WITH_H3=OFF` returns a controlled unsupported error for these functions.

The `WITH_H3` BE build option is enabled by default. A BE built with `WITH_H3=OFF` returns a controlled unsupported error for these functions.

## Resource limits

These mutable BE settings are checked per row before H3 allocation/fill. Exceeding a limit raises an error; results are never truncated. Query and column memory limits still apply.

| BE setting | Default | Unit and meaning |
| --- | ---: | --- |
| `h3_max_cells_per_row` | 100000 | Output cells and summed expansion slots before deduplication. |
| `h3_max_grid_disk_k` | 128 | Largest allowed grid distance. |
| `h3_max_polygon_vertices` | 10000 | Coordinate positions across all rings/components, including closing positions. |
| `h3_max_polygon_components` | 256 | Polygons in a row, including empty components. |
| `h3_max_working_bytes` | 67108864 | Bytes of worker preparation and temporary buffers (64 MiB). |
| `h3_max_estimated_work_per_row` | 10000000 | Sum of `U Г— (V + 1)`, where `U` is H3's estimated fill slots and `V` is component coordinate positions. This is an admission heuristic, not a CPU time bound. |

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
