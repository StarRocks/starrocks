---
displayed_sidebar: docs
description: "Constructs a native GEOMETRY value from WKB with an explicit CRS."
---

# ST_GeomFromWKB

Constructs a native planar `GEOMETRY` value from two-dimensional OGC Well-Known Binary (WKB).

## Syntax

```Haskell
GEOMETRY ST_GeomFromWKB(VARBINARY wkb, VARCHAR crs)
```

`crs` must be a non-empty string literal. There is no default CRS and no implicit coordinate conversion or reprojection. Both WKB byte orders, all seven 2D OGC geometry families, `EMPTY`, and empty children are supported. EWKB and Z/M ordinates are not supported. Invalid WKB returns `NULL`; a `NULL` WKB returns `NULL`.

## Example

```SQL
SELECT ST_AsText(ST_GeomFromWKB(
    ST_AsBinary(ST_GeomFromText('POINT (1 2)', 'EPSG:3857')),
    'EPSG:3857'));
```

```text
POINT (1 2)
```

## Keywords

ST_GEOMFROMWKB, GEOMETRY, WKB, CRS
