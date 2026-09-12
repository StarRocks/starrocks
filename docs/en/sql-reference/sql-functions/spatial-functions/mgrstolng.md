---
displayed_sidebar: docs
description: "Returns the longitude of the center point of the grid square identified by an MGRS string."
---

# MGRSToLng

Decodes a [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) string and returns the longitude of the center of the referenced grid square. This is one half of the inverse of `geoToMGRS`; see also `MGRSToLat`.

Input is case-insensitive and whitespace is ignored.

## Syntax

```Haskell
DOUBLE MGRSToLng(VARCHAR mgrs)
```

## Parameters

- `mgrs`: An MGRS reference string, e.g. `'31UDQ4825111935'`. Supported data type: VARCHAR.

## Return value

Returns a DOUBLE representing the longitude in degrees (WGS84) of the center of the grid square. Returns NULL if the argument is NULL or if the string is malformed.

## Examples

```sql
SELECT MGRSToLng('31UDQ4825111935');
+-----------------------------+
| MGRSToLng('31UDQ4825111935')|
+-----------------------------+
| 2.294495618908297           |
+-----------------------------+
```

## keyword

MGRSTOLNG,MGRS,GEO,SPATIAL,UTM,LONGITUDE
