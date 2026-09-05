---
displayed_sidebar: docs
description: "Returns the longitude of the center point of an H3 cell given its BIGINT index."
---

# h3ToGeoLng

Returns the longitude (in degrees, WGS84) of the center point of the H3 cell identified by the given index.

## Syntax

```Haskell
DOUBLE h3ToGeoLng(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns a DOUBLE representing the longitude of the H3 cell center in degrees. Returns NULL if the argument is NULL or if the index is not a valid H3 cell.

## Examples

```sql
SELECT h3ToGeoLng(617700169958293503);
+--------------------------------+
| h3ToGeoLng(617700169958293503) |
+--------------------------------+
|          -122.41935029526676   |
+--------------------------------+
```

## keyword

H3TOGEOLONG,H3TOGEOLONG,H3,GEO,SPATIAL,LONGITUDE
