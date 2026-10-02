---
displayed_sidebar: docs
description: "Returns the latitude of the center point of the grid square identified by an MGRS string."
---

# MGRSToLat

Decodes a [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) string and returns the latitude of the center of the referenced grid square. This is one half of the inverse of `geoToMGRS`; see also `MGRSToLng`.

Input is case-insensitive and whitespace is ignored.

## Syntax

```Haskell
DOUBLE MGRSToLat(VARCHAR mgrs)
```

## Parameters

- `mgrs`: An MGRS reference string, e.g. `'31UDQ4825111935'`. Supported data type: VARCHAR.

## Return value

Returns a DOUBLE representing the latitude in degrees (WGS84) of the center of the grid square. Returns NULL if the argument is NULL or if the string is malformed.

## Examples

```sql
SELECT MGRSToLat('31UDQ4825111935');
+-----------------------------+
| MGRSToLat('31UDQ4825111935')|
+-----------------------------+
| 48.85822536113692           |
+-----------------------------+
```

## keyword

MGRSTOLAT,MGRS,GEO,SPATIAL,UTM,LATITUDE
