---
displayed_sidebar: docs
description: "Encodes WGS84 longitude/latitude coordinates as a Military Grid Reference System (MGRS) string."
---

# geoToMGRS

Encodes a WGS84 (longitude, latitude) point as a [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) string. The optional `precision` argument controls the number of easting/northing digits: 5 (default) gives 1 m resolution, down to 0 for the 100 km grid square only. Only latitudes in the UTM domain [−80°, 84°] are supported.

## Syntax

```Haskell
VARCHAR geoToMGRS(DOUBLE longitude, DOUBLE latitude[, INT precision])
```

## Parameters

- `longitude`: Longitude in degrees. Range: `[-180, 180]`. Supported data type: DOUBLE.
- `latitude`: Latitude in degrees. Range: `[-80, 84]`. Supported data type: DOUBLE.
- `precision`: Optional. Number of easting/northing digits each. Range: `[0, 5]`, default `5`. Supported data type: INT.

| precision | Resolution | Example output      |
|-----------|------------|---------------------|
| 5         | 1 m        | `31UDQ4825111935`   |
| 4         | 10 m       | `31UDQ48251193`     |
| 3         | 100 m      | `31UDQ482119`       |
| 2         | 1 km       | `31UDQ4811`         |
| 1         | 10 km      | `31UDQ41`           |
| 0         | 100 km     | `31UDQ`             |

## Return value

Returns a VARCHAR MGRS reference string. Returns NULL if `latitude` is outside `[-80, 84]`, `longitude` is outside `[-180, 180]`, `precision` is outside `[0, 5]`, or any argument is NULL.

## Examples

Example 1: Encode the Eiffel Tower at full (1 m) precision.

```sql
SELECT geoToMGRS(2.294497, 48.858222);
+--------------------------------+
| geoToMGRS(2.294497, 48.858222) |
+--------------------------------+
| 31UDQ4825111935                |
+--------------------------------+
```

Example 2: Encode at 100 m precision.

```sql
SELECT geoToMGRS(2.294497, 48.858222, 3);
+-----------------------------------+
| geoToMGRS(2.294497, 48.858222, 3) |
+-----------------------------------+
| 31UDQ482119                       |
+-----------------------------------+
```

Example 3: Use with `MGRSToLat` and `MGRSToLng` to round-trip coordinates.

```sql
SELECT MGRSToLat(geoToMGRS(2.294497, 48.858222)) AS lat,
       MGRSToLng(geoToMGRS(2.294497, 48.858222)) AS lng;
+--------------------+--------------------+
| lat                | lng                |
+--------------------+--------------------+
| 48.85822536113692  | 2.294495618908297  |
+--------------------+--------------------+
```

## keyword

GEOTOMGRS,MGRS,GEO,SPATIAL,UTM
