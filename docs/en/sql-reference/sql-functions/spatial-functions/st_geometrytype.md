---
displayed_sidebar: docs
description: "Returns the OGC geometry family of a GEOGRAPHY value."
---

# ST_GEOMETRYTYPE

Returns the Open Geospatial Consortium (OGC) geometry family of a `GEOGRAPHY` value.

## Syntax

```SQL
VARCHAR ST_GEOMETRYTYPE(GEOGRAPHY geography)
```

## Return value

Returns one of `ST_Point`, `ST_LineString`, `ST_Polygon`, `ST_MultiPoint`, `ST_MultiLineString`, `ST_MultiPolygon`, or `ST_GeometryCollection`. A typed EMPTY value retains its family name. Returns `NULL` when the input is `NULL`.

Unsupported dimensions or descriptors produce an error. A `GEOMETRY` overload is not supported.

## Examples

```SQL
SELECT ST_GEOMETRYTYPE(ST_GeogFromText('LINESTRING (0 0, 1 1)'));
-- ST_LineString

SELECT ST_GEOMETRYTYPE(ST_GeogFromText('POINT EMPTY'));
-- ST_Point
```

## keyword

ST_GEOMETRYTYPE,ST,GEOMETRYTYPE
