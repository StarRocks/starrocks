---
displayed_sidebar: docs
description: "Tests whether a point is covered by a polygon, including its boundary."
---

# ST_CoveredBy

Returns whether a point is in a polygon interior or on its exterior or hole boundary. This is the converse of `ST_Covers`.

## Syntax

```SQL
ST_CoveredBy(GEOGRAPHY point, GEOGRAPHY polygon)
ST_CoveredBy(GEOMETRY point, GEOMETRY polygon)
```

`polygon` must be an XY `POLYGON` or `MULTIPOLYGON`, and `point` must be an XY `POINT`. `GEOMETRY` descriptors must match. `NULL` propagates, and an `EMPTY` input returns `false`.

## Example

```SQL
SELECT ST_CoveredBy(
    ST_GeomFromText('POINT (0 5)', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'));
-- true
```

## Keywords

ST_COVEREDBY,ST,COVEREDBY
