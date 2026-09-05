---
displayed_sidebar: docs
description: "Returns the Haversine (great-circle) distance in metres between two geographic points."
---

# h3PointDistM

Returns the Haversine (great-circle) distance in metres between two geographic points specified by their latitude and longitude in degrees.

## Syntax

```Haskell
DOUBLE h3PointDistM(DOUBLE lat1, DOUBLE lon1, DOUBLE lat2, DOUBLE lon2)
```

## Parameters

- `lat1`: Latitude of the first point in degrees. Supported data type: DOUBLE.
- `lon1`: Longitude of the first point in degrees. Supported data type: DOUBLE.
- `lat2`: Latitude of the second point in degrees. Supported data type: DOUBLE.
- `lon2`: Longitude of the second point in degrees. Supported data type: DOUBLE.

## Return value

Returns a DOUBLE representing the great-circle distance in metres between the two points. Returns NULL if any argument is NULL.

## Examples

```sql
SELECT h3PointDistM(-10.0, 0.0, 10.0, 0.0);
+--------------------------------------+
| h3PointDistM(-10.0, 0.0, 10.0, 0.0) |
+--------------------------------------+
| 2223901.039504589                    |
+--------------------------------------+
```

## keyword

H3POINTDISTM,H3,SPATIAL
