---
displayed_sidebar: docs
description: "Constructs a native GEOGRAPHY value from WKT."
products: [oss, enterprise, cloud, anywhere]
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

In queries, compatible native geo values can be used as `ARRAY` elements, `MAP` values, or `STRUCT` fields. You can construct these containers, read elements and fields, use `cardinality` or `array_length`, expand arrays with `UNNEST`, and use `array_map`, `array_filter`, or unordered `array_agg` and `map_agg` without `DISTINCT`.

A `MAP` key cannot contain geo values. Operations that require geo equality, ordering, or hashing are rejected, including comparisons, sorting, and deduplication. Saved SQL definitions, including views, materialized views, SQL UDFs, generated columns, SQL plan baselines, and pipes, cannot use nested geo expressions.

To return a container to the client, convert its geo elements or fields with `ST_AsText` or `ST_AsBinary` first. Scalar `GEOGRAPHY` can be returned directly. Scalar `GEOMETRY` also requires explicit conversion.

```sql
SELECT array_map(x -> ST_AsText(x), [ST_GeogFromText('POINT (1 2)')]);
```

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
