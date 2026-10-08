---
displayed_sidebar: docs
description: "Tests whether two native polygons or multipolygons intersect."
---

# ST_Intersects

Returns whether two native polygons or multipolygons share any point. Interior overlap, containment, and contact at an exterior or hole boundary return `true`. A polygon located entirely inside a hole returns `false`.

## Syntax

```SQL
ST_Intersects(GEOGRAPHY lhs, GEOGRAPHY rhs)
ST_Intersects(GEOMETRY lhs, GEOMETRY rhs)
```

Both arguments must be XY `POLYGON` or `MULTIPOLYGON` values of the same native logical type. `GEOGRAPHY` uses spherical OGC:CRS84 edges. `GEOMETRY` uses planar edges, and both arguments must have compatible descriptors. The function does not transform coordinates.

`NULL` propagates, and an `EMPTY` input returns `false`. Unsupported geometry families, `GEOMETRYCOLLECTION`, mixed `GEOGRAPHY`/`GEOMETRY` inputs, and incompatible `GEOMETRY` descriptors produce an error.

## Examples

```SQL
SELECT ST_Intersects(
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((10 0, 20 0, 20 10, 10 10, 10 0))', 'EPSG:3857'));
-- true: the polygons share an edge

SELECT ST_Intersects(
    ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((4 4, 6 4, 6 6, 4 6, 4 4))', 'EPSG:3857'));
-- false: the second polygon is inside the hole
```

## Keywords

ST_INTERSECTS,ST,INTERSECTS,GEOGRAPHY,GEOMETRY
