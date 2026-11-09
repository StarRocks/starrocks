---
displayed_sidebar: docs
description: "Returns the X coordinate of a GEOGRAPHY or GEOMETRY point."
---

# ST_X

Returns the X coordinate of a point.

For a `GEOGRAPHY` value, X is longitude in degrees under the OGC:CRS84 spherical contract. For a `GEOMETRY` value, X is returned in the units of its declared CRS and is not necessarily longitude. The value must be a non-empty `POINT`.

## Syntax

```SQL
DOUBLE ST_X(VARCHAR point)
DOUBLE ST_X(GEOGRAPHY point)
DOUBLE ST_X(GEOMETRY point)
```

## Return value

Returns `NULL` when the input is `NULL`. For native `GEOGRAPHY` and `GEOMETRY`, an empty value or a non-`POINT` family produces an error. Native compute supports XY values with a valid descriptor.

## Examples

```SQL
SELECT ST_X(ST_Point(24.7, 56.7));
-- 24.7

SELECT ST_X(ST_GeogFromText('POINT (24.7 56.7)'));
-- 24.7

SELECT ST_X(ST_GeomFromText('POINT (1000000.5 2000000)', 'EPSG:3857'));
-- 1000000.5
```

## keyword

ST_X,ST,X
