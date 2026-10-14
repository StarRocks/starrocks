---
displayed_sidebar: docs
description: "Tests whether a point is strictly within a polygon."
---

# ST_Within

Returns whether a point is in the interior of a polygon. This is the converse of `ST_Contains` and excludes exterior and hole boundaries.

## Syntax

```SQL
ST_Within(GEOGRAPHY point, GEOGRAPHY polygon)
ST_Within(GEOMETRY point, GEOMETRY polygon)
```

`polygon` must be an XY `POLYGON` or `MULTIPOLYGON`, and `point` must be an XY `POINT`. `GEOMETRY` descriptors must match. `NULL` propagates, and an `EMPTY` input returns `false`.

## Example

```SQL
SELECT ST_Within(
    ST_GeomFromText('POINT (5 5)', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'));
-- true
```

## Keywords

ST_WITHIN,ST,WITHIN
