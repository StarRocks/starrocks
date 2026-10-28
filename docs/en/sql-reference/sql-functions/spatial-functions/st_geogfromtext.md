---
displayed_sidebar: docs
description: "Constructs a native GEOGRAPHY value from WKT."
---

# ST_GeogFromText

Constructs a native `GEOGRAPHY` value from Well-Known Text (WKT).

## Syntax

```Haskell
GEOGRAPHY ST_GeogFromText(VARCHAR wkt)
GEOGRAPHY ST_GeogFromText(VARCHAR wkt, INT srid)
```

## Parameters

- `wkt`: A two-dimensional OGC WKT value. All seven OGC geometry families and `EMPTY` members are supported.
- `srid`: Optional. The spatial reference identifier. Only `4326` is supported; when omitted, it defaults to `4326`.

Coordinates use spherical `OGC:CRS84` semantics: longitude must be in `[-180, 180]` and latitude in `[-90, 90]`.

## Return value

Returns a `GEOGRAPHY` value. Returns `NULL` if an argument is `NULL`, the WKT is invalid, a coordinate is outside the supported range, or `srid` is not `4326`.

## Examples

```SQL
SELECT ST_AsText(ST_GeogFromText('LINESTRING (1 2, 3 4)', 4326));
```

```text
LINESTRING (1 2, 3 4)
```

```SQL
SELECT ST_AsText(ST_GeogFromText('POINT EMPTY'));
```

```text
POINT EMPTY
```

## Keywords

ST_GEOGFROMTEXT, GEOGRAPHY, WKT
