---
displayed_sidebar: docs
description: "Returns the Y coordinate of a GEOGRAPHY or GEOMETRY point."
---

# ST_Y

Returns the Y coordinate of a point.

For a `GEOGRAPHY` value, Y is latitude in degrees under the OGC:CRS84 spherical contract. For a `GEOMETRY` value, Y is returned in the units of its declared CRS and is not necessarily latitude. The value must be a non-empty `POINT`.

## Syntax

```SQL
DOUBLE ST_Y(VARCHAR point)
DOUBLE ST_Y(GEOGRAPHY point)
DOUBLE ST_Y(GEOMETRY point)
```

## Return value

Returns `NULL` when the input is `NULL`. For native `GEOGRAPHY` and `GEOMETRY`, an empty value or a non-`POINT` family produces an error. Native compute supports XY values with a valid descriptor.

## Examples

```SQL
SELECT ST_Y(ST_Point(24.7, 56.7));
-- 56.7

SELECT ST_Y(ST_GeogFromText('POINT (24.7 56.7)'));
-- 56.7

SELECT ST_Y(ST_GeomFromText('POINT (1000000 2000000.5)', 'EPSG:3857'));
-- 2000000.5
```

## keyword

ST_Y,ST,Y
