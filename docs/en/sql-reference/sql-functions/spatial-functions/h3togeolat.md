---
displayed_sidebar: docs
description: "Returns the latitude of the center point of an H3 cell given its BIGINT index."
---

# h3ToGeoLat

Returns the latitude (in degrees, WGS84) of the center point of the H3 cell identified by the given index.

## Syntax

```Haskell
DOUBLE h3ToGeoLat(BIGINT h3index)
```

## Parameters

- `h3index`: An H3 cell index. Supported data type: BIGINT.

## Return value

Returns a DOUBLE representing the latitude of the H3 cell center in degrees. Returns NULL if the argument is NULL or if the index is not a valid H3 cell.

## Examples

```sql
SELECT h3ToGeoLat(617700169958293503);
+--------------------------------+
| h3ToGeoLat(617700169958293503) |
+--------------------------------+
|            37.77492951615992   |
+--------------------------------+
```

## keyword

H3TOGEOLAT,H3,GEO,SPATIAL,LATITUDE
