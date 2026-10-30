---
displayed_sidebar: docs
description: "Tests whether a polygon strictly contains a point."
---

# ST_Contains

Returns whether a polygon contains a point in its interior. A point on the exterior boundary or on a hole boundary is not contained.

## Syntax

```SQL
ST_Contains(GEOGRAPHY polygon, GEOGRAPHY point)
ST_Contains(GEOMETRY polygon, GEOMETRY point)
ST_Contains(VARCHAR shape1, VARCHAR shape2)
```

The native overloads accept an XY `POINT` as the second argument and an XY `POLYGON` or `MULTIPOLYGON` as the first. Both `GEOMETRY` arguments must have matching descriptors. Mixed native `GEOGRAPHY`/`GEOMETRY` calls are rejected. `NULL` propagates, and an `EMPTY` input returns `false`.

The `VARCHAR` signature is the existing legacy overload and remains unchanged.

## Examples

```SQL
SELECT ST_Contains(
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POINT (5 5)', 'EPSG:3857'));
-- true

SELECT ST_Contains(
    ST_GeogFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),
    ST_GeogFromText('POINT (0 5)'));
-- false: the point is on the boundary
```

## Keywords

ST_CONTAINS,ST,CONTAINS
