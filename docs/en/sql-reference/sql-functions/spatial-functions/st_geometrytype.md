---
displayed_sidebar: docs
description: "Returns the OGC geometry family of a GEOGRAPHY or GEOMETRY value."
---

# ST_GEOMETRYTYPE

Returns the Open Geospatial Consortium (OGC) geometry family of a `GEOGRAPHY` or `GEOMETRY` value.

## Syntax

```SQL
VARCHAR ST_GEOMETRYTYPE(GEOGRAPHY geography)
VARCHAR ST_GEOMETRYTYPE(GEOMETRY geometry)
```

## Return value

Returns one of `ST_Point`, `ST_LineString`, `ST_Polygon`, `ST_MultiPoint`, `ST_MultiLineString`, `ST_MultiPolygon`, or `ST_GeometryCollection`. A typed EMPTY value retains its family name. Returns `NULL` when the input is `NULL`.

Native compute supports XY values with a valid descriptor. Unsupported dimensions or descriptors produce an error.

## Examples

```SQL
SELECT ST_GEOMETRYTYPE(ST_GeogFromText('LINESTRING (0 0, 1 1)'));
-- ST_LineString

SELECT ST_GEOMETRYTYPE(ST_GeogFromText('POINT EMPTY'));
-- ST_Point

SELECT ST_GEOMETRYTYPE(
    ST_GeomFromText('GEOMETRYCOLLECTION (POINT (0 0))', 'EPSG:3857'));
-- ST_GeometryCollection
```

## keyword

ST_GEOMETRYTYPE,ST,GEOMETRYTYPE
