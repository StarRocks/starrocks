---
displayed_sidebar: docs
description: "Returns the two-dimensional area of a native GEOGRAPHY or GEOMETRY value."
---

# ST_Area

Returns the area of the two-dimensional components of a native GEO value. `GEOGRAPHY` results are square meters on the StarRocks spherical Earth model. `GEOMETRY` results are squared units of the input CRS. No reprojection is performed.

## Syntax

```SQL
ST_Area(GEOGRAPHY value)
ST_Area(GEOMETRY value)
```

`POLYGON` and `MULTIPOLYGON` contribute their filled area, with holes subtracted. Polygon components inside a `GEOMETRYCOLLECTION` are summed recursively. Points and lines contribute zero. Overlapping polygon components make the value topologically invalid and produce an error instead of being double-counted.

For `GEOGRAPHY`, ring winding does not select the interior. Each ring is normalized to the smaller spherical region, so a single ring cannot represent an area larger than a hemisphere. Holes and multipolygon components follow the same rule.

`NULL` produces `NULL`; an `EMPTY` value produces `0`. Malformed WKB, unsupported dimensions or descriptors, and topologically invalid inputs produce a controlled error.

## Examples

```SQL
SELECT ST_Area(ST_GeomFromText(
    'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))',
    'EPSG:3857'));
-- 15

SELECT ST_Area(ST_GeogFromText('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'));
-- approximately 12364036567.0764 square meters
```

## Keywords

ST_AREA,ST,AREA,GEOGRAPHY,GEOMETRY
