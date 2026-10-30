---
displayed_sidebar: docs
sidebar_position: 52
description: "Reprojects native GEOMETRY coordinates between EPSG:4326 and EPSG:3857."
---

# ST_Transform

Reprojects a native `GEOMETRY` between EPSG:4326 (longitude/latitude) and EPSG:3857 (Web Mercator). The result descriptor records the target CRS; unlike [ST_SetSRID](st_setsrid.md), this function changes coordinates. The conversion uses the Web Mercator formulas and requires no external CRS library.

## Syntax

```SQL
GEOMETRY ST_Transform(GEOMETRY value, INT target_srid)
```

`target_srid` must be a FE-foldable constant equal to 4326 or 3857. The input CRS must be EPSG:4326, EPSG:3857, or OGC:CRS84 (an alias of EPSG:4326 for this function). SQL X/Y for EPSG:4326 means longitude/latitude. A matching source and target SRID leaves coordinates unchanged.

The forward conversion accepts longitude from -180 to 180 degrees and latitude from approximately -85.05112878 to 85.05112878 degrees. The inverse conversion accepts Web Mercator X/Y in approximately ±20037508.3427893 meters. Coordinates outside those bounds, unsupported CRSs, malformed WKB, and inconsistent descriptors produce an error.

The function transforms every coordinate in the seven XY OGC geometry families, including nested collections. `EMPTY` remains empty and `NULL` returns `NULL`. Z/M ordinates and `GEOGRAPHY` are not supported.

## Example

```SQL
SELECT ST_X(ST_Transform(
    ST_GeomFromText('POINT (10 20)', 'EPSG:4326'), 3857));
-- approximately 1113194.90793274 meters

SELECT ST_Y(ST_Transform(
    ST_GeomFromText('POINT (10 20)', 'EPSG:4326'), 3857));
-- approximately 2273030.92698769 meters
```

## Keywords

ST_TRANSFORM, GEOMETRY, CRS, EPSG, Web Mercator
