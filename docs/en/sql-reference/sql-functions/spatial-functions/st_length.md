---
displayed_sidebar: docs
description: "Returns the length of the one-dimensional components of a native GEO value."
---

# ST_Length

Returns the total length of the one-dimensional components of a native GEO value. `GEOGRAPHY` results are meters along great-circle edges. `GEOMETRY` results use the coordinate units of the input CRS.

## Syntax

```SQL
ST_Length(GEOGRAPHY value)
ST_Length(GEOMETRY value)
```

`LINESTRING` and `MULTILINESTRING` contribute their segment lengths. Line components inside a `GEOMETRYCOLLECTION` are summed recursively. Points and polygon boundaries contribute zero; use `ST_Perimeter` for polygon boundaries.

`NULL` produces `NULL`; an `EMPTY` value produces `0`. Malformed WKB, unsupported dimensions or descriptors, ambiguous antipodal spherical segments, and topologically invalid inputs produce a controlled error.

## Examples

```SQL
SELECT ST_Length(ST_GeomFromText('LINESTRING (0 0, 3 4)', 'EPSG:3857'));
-- 5

SELECT ST_Length(ST_GeogFromText('LINESTRING (0 0, 1 0)'));
-- approximately 111195.101177484 meters
```

## Keywords

ST_LENGTH,ST,LENGTH,GEOGRAPHY,GEOMETRY
