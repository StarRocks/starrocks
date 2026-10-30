---
displayed_sidebar: docs
description: "Constructs a native GEOGRAPHY value from WKB."
---

# ST_GeogFromWKB

Constructs a native `GEOGRAPHY` value from Well-Known Binary (WKB).

## Syntax

```Haskell
GEOGRAPHY ST_GeogFromWKB(VARBINARY wkb)
GEOGRAPHY ST_GeogFromWKB(VARBINARY wkb, INT srid)
```

## Parameters

- `wkb`: A two-dimensional OGC WKB value in little-endian or big-endian byte order. All seven OGC geometry families and `EMPTY` members are supported. EWKB extensions are not supported.
- `srid`: Optional. The spatial reference identifier. Only `4326` is supported; when omitted, it defaults to `4326`.

Coordinates use spherical `OGC:CRS84` semantics: longitude must be in `[-180, 180]` and latitude in `[-90, 90]`.

## Return value

Returns a `GEOGRAPHY` value. Returns `NULL` if an argument is `NULL`, the WKB is invalid, a coordinate is outside the supported range, or `srid` is not `4326`.

## Example

```SQL
SELECT ST_AsText(ST_GeogFromWKB(unhex('0101000000000000000000F03F0000000000000040')));
```

```text
POINT (1 2)
```

## Keywords

ST_GEOGFROMWKB, GEOGRAPHY, WKB
