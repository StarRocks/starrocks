---
displayed_sidebar: docs
description: "Converts OGC WKB to a native GEOMETRY value."
---

# ST_GeomFromWKB, ST_GeometryFromWKB

Converts a two-dimensional OGC Well-Known Binary (WKB) value to a native `GEOMETRY` value.

## Syntax

```Haskell
GEOMETRY ST_GeomFromWKB(VARBINARY wkb)
GEOMETRY ST_GeometryFromWKB(VARBINARY wkb)
```

Both little-endian and big-endian OGC WKB input are accepted. The function supports all seven OGC geometry families and their `EMPTY` forms. EWKB, SRID, and Z/M ordinates are not supported. Invalid input returns `NULL`.

## Example

```SQL
SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT (1 2)'))));
-- POINT (1 2)
```

## keyword

ST_GEOMFROMWKB,ST_GEOMETRYFROMWKB,ST,GEOMFROMWKB,GEOMETRYFROMWKB
