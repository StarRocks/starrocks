---
displayed_sidebar: docs
sidebar_position: 50
description: "Returns the numeric EPSG SRID recorded in a native GEOMETRY descriptor."
---

# ST_SRID

Returns the numeric SRID associated with a native `GEOMETRY` CRS. `OGC:CRS84` maps to 4326. A CRS without an unambiguous numeric EPSG mapping returns `NULL`; the function never substitutes EPSG:0.

## Syntax

```SQL
INT ST_SRID(GEOMETRY value)
```

A `NULL` input returns `NULL`. This function reads metadata and does not transform coordinates. It does not accept `GEOGRAPHY`.

## Example

```SQL
SELECT ST_SRID(ST_GeomFromText('POINT (10 20)', 'EPSG:4326'));
-- 4326
```

## Keywords

ST_SRID, GEOMETRY, CRS, EPSG
