---
displayed_sidebar: docs
description: "Serializes a native GEOGRAPHY value as WKB."
---

# ST_AsBinary, ST_AsWKB

Serializes a native `GEOGRAPHY` value as Well-Known Binary (WKB). `ST_AsWKB` is an alias of `ST_AsBinary`.

## Syntax

```Haskell
VARBINARY ST_AsBinary(GEOGRAPHY geography)
VARBINARY ST_AsWKB(GEOGRAPHY geography)
```

The function preserves all seven OGC geometry families and `EMPTY` members. A `NULL` input returns `NULL`.

## Example

```SQL
SELECT hex(ST_AsBinary(ST_GeogFromText('POINT (1 2)')));
```

```text
0101000000000000000000F03F0000000000000040
```

## Keywords

ST_ASBINARY, ST_ASWKB, GEOGRAPHY, WKB
