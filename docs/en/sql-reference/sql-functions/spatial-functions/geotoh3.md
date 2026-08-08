---
displayed_sidebar: docs
description: "Converts a WGS84 longitude/latitude coordinate to an H3 cell index (BIGINT) at the given resolution."
---

# geoToH3

Converts a WGS84 longitude/latitude point to an H3 hexagonal cell index at the specified resolution. The returned value is a BIGINT representation of the 64-bit H3 cell index, compatible with ClickHouse, Snowflake, and Databricks H3 semantics.

## Syntax

```Haskell
BIGINT geoToH3(DOUBLE lng, DOUBLE lat, INT resolution)
```

## Parameters

- `lng`: Longitude of the point in degrees (WGS84). Supported data type: DOUBLE.
- `lat`: Latitude of the point in degrees (WGS84). Supported data type: DOUBLE.
- `resolution`: H3 resolution level, in the range [0, 15]. Higher values produce smaller, more precise cells. Supported data type: INT.

## Return value

Returns a BIGINT representing the H3 cell index that contains the given point. Returns NULL if any argument is NULL, if coordinates are out of valid range, or if the resolution is outside [0, 15].

## Examples

Example 1: Get the H3 cell index for a location in San Francisco at resolution 9.

```sql
SELECT geoToH3(-122.4194, 37.7749, 9);
+--------------------------------+
| geoToH3(-122.4194, 37.7749, 9) |
+--------------------------------+
|          617700169958293503    |
+--------------------------------+
```

Example 2: Verify that the returned index is valid and has the expected resolution.

```sql
SELECT h3IsValid(geoToH3(-122.4194, 37.7749, 9)),
       h3GetResolution(geoToH3(-122.4194, 37.7749, 9));
+-------------------------------------------+--------------------------------------------------+
| h3IsValid(geoToH3(-122.4194, 37.7749, 9)) | h3GetResolution(geoToH3(-122.4194, 37.7749, 9)) |
+-------------------------------------------+--------------------------------------------------+
|                                         1 |                                                9 |
+-------------------------------------------+--------------------------------------------------+
```

Example 3: Returns NULL for an invalid resolution.

```sql
SELECT geoToH3(-122.4194, 37.7749, 16);
+---------------------------------+
| geoToH3(-122.4194, 37.7749, 16) |
+---------------------------------+
|                            NULL |
+---------------------------------+
```

## keyword

GEOTOH3,H3,GEO,SPATIAL
