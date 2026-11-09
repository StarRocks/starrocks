---
displayed_sidebar: docs
description: "Converts a GEOMETRY or GEOGRAPHY value to WKT (Well-Known Text)."
---

# ST_AsText, ST_AsWKT



Converts a `GEOMETRY` or `GEOGRAPHY` value to WKT (Well-Known Text). `ST_AsWKT` is an alias of `ST_AsText`.

## Syntax

```Haskell
VARCHAR ST_AsText(GEOMETRY geo)
VARCHAR ST_AsText(GEOGRAPHY geography)
VARCHAR ST_AsWKT(GEOMETRY geo)
VARCHAR ST_AsWKT(GEOGRAPHY geography)
```

For native `GEOMETRY` and `GEOGRAPHY`, the function preserves all seven OGC geometry families, `EMPTY`, and empty children. A `NULL` input returns `NULL`. The serializer does not change the input CRS or coordinates.

## Examples

```SQL
SELECT ST_AsText(ST_GeogFromText('GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (1 2, 3 4))'));
```

```text
GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (1 2, 3 4))
```

```SQL
SELECT ST_AsText(ST_GeomFromText('POINT (1000000 2000000)', 'EPSG:3857'));
```

```text
POINT (1000000 2000000)
```

```Plain Text
MySQL > SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```

## keyword

ST_ASTEXT, ST_ASWKT, ST, ASTEXT, ASWKT
