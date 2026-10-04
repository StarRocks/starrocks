---
displayed_sidebar: docs
description: "Tests the topology validity of a native GEOGRAPHY or GEOMETRY value."
---

# ST_IsValid

Returns whether a structurally readable native GEO value is topologically valid. It never repairs or rewrites the input.

## Syntax

```SQL
ST_IsValid(GEOGRAPHY value)
ST_IsValid(GEOMETRY value)
```

All seven XY OGC families are supported, including `MULTI*` and `GEOMETRYCOLLECTION`. `GEOMETRY` uses robust planar topology checks. `GEOGRAPHY` validates longitude/latitude, great-circle lines, spherical polygon loops and holes, and rejects multipolygon components with overlapping interiors. Its result can differ from planar validity for the same coordinates.

A topology-invalid but readable value returns `false`. `EMPTY` values return `true`, and SQL `NULL` produces `NULL`. Malformed WKB, unsupported dimensions, and invalid or unsupported descriptors produce a controlled error rather than `false`.

## Examples

```SQL
SELECT ST_IsValid(ST_GeomFromText(
    'POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))', 'EPSG:3857'));
-- false: the ring self-intersects

SELECT ST_IsValid(ST_GeogFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))'));
-- true
```

## Keywords

ST_ISVALID,ST,ISVALID,VALID,GEOGRAPHY,GEOMETRY
