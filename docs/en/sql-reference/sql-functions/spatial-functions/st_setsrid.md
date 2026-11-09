---
displayed_sidebar: docs
sidebar_position: 51
description: "Changes native GEOMETRY CRS metadata without transforming coordinates."
---

# ST_SetSRID

Assigns EPSG:4326 or EPSG:3857 CRS metadata to native `GEOMETRY`. The WKB bytes and every coordinate remain unchanged; use [ST_Transform](st_transform.md) to reproject coordinates.

## Syntax

```SQL
GEOMETRY ST_SetSRID(GEOMETRY value, INT target_srid)
```

`target_srid` must be a FE-foldable constant equal to 4326 or 3857. The source CRS must be EPSG:4326, EPSG:3857, or OGC:CRS84. This restricted set keeps the CRS metadata two-dimensional without requiring an external CRS database. SQL coordinates use X/Y order, or longitude/latitude for EPSG:4326. Only XY WKB is supported, across the seven OGC geometry families including collections and `EMPTY`. A `NULL` geometry returns `NULL`; malformed WKB and conflicting CRS metadata produce an error. `GEOGRAPHY` is not accepted.

## Example

```SQL
SELECT ST_SRID(ST_SetSRID(
    ST_GeomFromText('POINT (10 20)', 'EPSG:4326'), 3857));
-- 3857; the point is still at (10, 20)
```

## Keywords

ST_SETSRID, GEOMETRY, CRS, EPSG
