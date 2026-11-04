---
displayed_sidebar: docs
description: "Returns the minimum distance between supported GEOGRAPHY or GEOMETRY values."
---

# ST_DISTANCE

Returns the minimum distance between two supported values. Point-to-line distance is measured to the closest point on any segment, including a segment interior and endpoints.

For `GEOGRAPHY`, the function evaluates spherical OGC:CRS84 edges and returns meters. For `GEOMETRY`, it evaluates planar edges and returns the units of the declared CRS. `GEOMETRY` inputs must have compatible descriptors. This function does not perform CRS transformation.

## Syntax

```SQL
DOUBLE ST_DISTANCE(GEOGRAPHY lhs, GEOGRAPHY rhs)
DOUBLE ST_DISTANCE(GEOMETRY lhs, GEOMETRY rhs)
```

## Parameters

The supported family pairs are:

- `POINT` with `POINT`
- `POINT` with `LINESTRING` or `MULTILINESTRING`, in either argument order

All values must be two-dimensional. An exact or numerically ambiguous antipodal segment in a `GEOGRAPHY` line is rejected because it does not define a unique shortest spherical edge.

## Return value

Returns `NULL` if either input is `NULL` or EMPTY. Unsupported families or descriptors, mixed `GEOGRAPHY`/`GEOMETRY` values, and incompatible `GEOMETRY` descriptors produce an error.

## Examples

```SQL
SELECT ST_DISTANCE(
    ST_GeogFromText('POINT (0 1)'),
    ST_GeogFromText('LINESTRING (-1 0, 1 0)'));
-- approximately 111195.1 meters

SELECT ST_DISTANCE(
    ST_GeomFromText('POINT (5 3)', 'EPSG:3857'),
    ST_GeomFromText('LINESTRING (0 0, 10 0)', 'EPSG:3857'));
-- 3
```

## keyword

ST_DISTANCE,ST,DISTANCE,GEOGRAPHY,GEOMETRY
