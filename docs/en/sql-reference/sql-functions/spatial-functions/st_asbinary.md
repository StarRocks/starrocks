---
displayed_sidebar: docs
description: "Serializes a native GEOMETRY value as canonical OGC WKB."
---

# ST_AsBinary, ST_AsWKB

Serializes a native `GEOMETRY` value as canonical little-endian OGC Well-Known Binary (WKB).

## Syntax

```Haskell
VARBINARY ST_AsBinary(GEOMETRY geo)
VARBINARY ST_AsWKB(GEOMETRY geo)
```

## Example

```SQL
SELECT HEX(ST_AsBinary(ST_GeomFromText('POINT (1 2)')));
-- 0101000000000000000000F03F0000000000000040
```

## keyword

ST_ASBINARY,ST_ASWKB,ST,ASBINARY,ASWKB
