---
displayed_sidebar: docs
description: "Tests whether a polygon covers a point, including its boundary."
---

# ST_Covers

Returns whether a point is in a polygon interior or on its exterior or hole boundary.

## Syntax

```SQL
ST_Covers(GEOGRAPHY polygon, GEOGRAPHY point)
ST_Covers(GEOMETRY polygon, GEOMETRY point)
```

`polygon` must be an XY `POLYGON` or `MULTIPOLYGON`, and `point` must be an XY `POINT`. `GEOMETRY` descriptors must match. `NULL` propagates, and an `EMPTY` input returns `false`.

## Example

```SQL
SELECT ST_Covers(
    ST_GeogFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),
    ST_GeogFromText('POINT (0 5)'));
-- true
```

## Keywords

ST_COVERS,ST,COVERS
