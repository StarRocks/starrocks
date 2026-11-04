---
displayed_sidebar: docs
description: "Returns the boundary length of polygon components in a native GEO value."
---

# ST_Perimeter

Returns the total boundary length of the two-dimensional components of a native GEO value. Exterior and interior rings are included. `GEOGRAPHY` results are meters along great-circle edges. `GEOMETRY` results use the coordinate units of the input CRS.

## Syntax

```SQL
ST_Perimeter(GEOGRAPHY value)
ST_Perimeter(GEOMETRY value)
```

`POLYGON` and `MULTIPOLYGON` contribute their ring lengths. Polygon components inside a `GEOMETRYCOLLECTION` are summed recursively. Points and lines contribute zero.

`NULL` produces `NULL`; an `EMPTY` value produces `0`. Malformed WKB, unsupported dimensions or descriptors, and topologically invalid inputs produce a controlled error. Spherical rings use the same smaller-region, winding-independent model described for `ST_Area`.

## Examples

```SQL
SELECT ST_Perimeter(ST_GeomFromText(
    'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 'EPSG:3857'));
-- 16

SELECT ST_Perimeter(ST_GeogFromText('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'));
-- approximately 444763.468727621 meters
```

## Keywords

ST_PERIMETER,ST,PERIMETER,GEOGRAPHY,GEOMETRY
