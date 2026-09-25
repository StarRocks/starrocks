---
displayed_sidebar: docs
description: "Returns the distance between two compatible GEOGRAPHY or GEOMETRY points."
---

# ST_DISTANCE

For `GEOGRAPHY`, returns the spherical distance in meters between two points under the OGC:CRS84 contract.

For `GEOMETRY`, returns the planar Euclidean distance in the units of the declared CRS. Both inputs must have compatible descriptors. Coordinates in an angular CRS are still treated as planar coordinate values; this overload does not perform a spherical calculation or CRS transformation.

## Syntax

```SQL
DOUBLE ST_DISTANCE(GEOGRAPHY lhs, GEOGRAPHY rhs)
DOUBLE ST_DISTANCE(GEOMETRY lhs, GEOMETRY rhs)
```

## Return value

Both values must be `POINT`. Returns `NULL` if either input is `NULL` or EMPTY. A non-`POINT` family, unsupported dimension, unsupported descriptor, mixed `GEOGRAPHY`/`GEOMETRY` pair, or incompatible `GEOMETRY` descriptors produce an error. Other geometry-family pairs are not supported.

## Examples

```SQL
SELECT ST_DISTANCE(
    ST_GeogFromText('POINT (0 0)'),
    ST_GeogFromText('POINT (1 0)'));
-- approximately 111195.1 meters

SELECT ST_DISTANCE(
    ST_GeomFromText('POINT (0 0)', 'EPSG:3857'),
    ST_GeomFromText('POINT (3 4)', 'EPSG:3857'));
-- 5
```

## keyword

ST_DISTANCE,ST,DISTANCE,GEOGRAPHY,GEOMETRY
