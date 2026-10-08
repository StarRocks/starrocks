---
displayed_sidebar: docs
description: "Tests whether a point is within an inclusive distance of a line or multiline."
---

# ST_DWITHIN

Returns whether the minimum distance between a `POINT` and a `LINESTRING` or `MULTILINESTRING` is less than or equal to a threshold. The arguments can be supplied in either order.

For `GEOGRAPHY`, the function evaluates spherical OGC:CRS84 edges and the threshold is in meters. For `GEOMETRY`, it evaluates planar edges and the threshold uses the units of the declared CRS. `GEOMETRY` inputs must have compatible descriptors. This function does not perform CRS transformation.

## Syntax

```SQL
BOOLEAN ST_DWITHIN(GEOGRAPHY point_or_line, GEOGRAPHY line_or_point, DOUBLE distance)
BOOLEAN ST_DWITHIN(GEOMETRY point_or_line, GEOMETRY line_or_point, DOUBLE distance)
```

## Parameters

- The geometry arguments must form a `POINT`/`LINESTRING` or `POINT`/`MULTILINESTRING` pair.
- `distance` must be finite and nonnegative. Equality is included.
- All geometry values must be two-dimensional.

An exact or numerically ambiguous antipodal segment in a `GEOGRAPHY` line is rejected because it does not define a unique shortest spherical edge.

## Return value

Returns `NULL` if a geometry argument or the threshold is `NULL`. Returns `false` if a geometry argument is EMPTY. Unsupported families or descriptors, an invalid threshold, mixed `GEOGRAPHY`/`GEOMETRY` values, and incompatible `GEOMETRY` descriptors produce an error.

## Examples

```SQL
SELECT ST_DWITHIN(
    ST_GeogFromText('POINT (0 1)'),
    ST_GeogFromText('LINESTRING (-1 0, 1 0)'),
    111196);
-- true

SELECT ST_DWITHIN(
    ST_GeomFromText('POINT (5 3)', 'EPSG:3857'),
    ST_GeomFromText('LINESTRING (0 0, 10 0)', 'EPSG:3857'),
    3);
-- true
```

## keyword

ST_DWITHIN,ST,DWITHIN,GEOGRAPHY,GEOMETRY
