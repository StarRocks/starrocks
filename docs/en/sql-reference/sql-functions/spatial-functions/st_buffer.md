---
displayed_sidebar: docs
description: "Returns a round Cartesian buffer around native XY geometry."
---

# ST_Buffer

Returns a round planar buffer at the signed distance from native `GEOMETRY`.

## Syntax

```SQL
ST_Buffer(GEOMETRY geometry, DOUBLE distance)
```

The distance can be constant or vary by row. There is no options or segment-count argument.

## Input and result

| Input family (XY) | Positive distance | Zero or negative distance |
| --- | --- | --- |
| POINT, MULTIPOINT | Round buffer | POLYGON EMPTY |
| LINESTRING, MULTILINESTRING | Round joins and end caps | POLYGON EMPTY |
| POLYGON, MULTIPOLYGON | Expansion | Zero preserves the valid area; negative erodes it |

The result is validated native XY `GEOMETRY` with the input CRS and planar edge semantics. One area component becomes `POLYGON`; multiple components become `MULTIPOLYGON`; an empty result is `POLYGON EMPTY`. Buffers can merge components, close holes, or split an eroded polygon. A single-member MULTIPOLYGON result is packed as POLYGON. Ring orientation, start vertex and component order may change.

`NULL` in either argument returns `NULL`. Typed EMPTY inputs, including EMPTY children, return POLYGON EMPTY for any finite distance. Non-NULL input rows must have valid structure and topology even when distance is zero or negative. Zero does not repair invalid polygons. Non-finite distances, invalid topology, malformed WKB and excessive input/output produce errors rather than repaired or truncated geometry.

`GEOGRAPHY`, GEOMETRYCOLLECTION (including EMPTY), Z/M coordinates, and unknown or mixed dimensions are unsupported. This is a scalar function, not an aggregate, window or array function.

## Units and approximation

Coordinates are Cartesian X/Y in the input CRS. EPSG:4326 uses degrees: a distance of 1000 means **1000 degrees**, and results can extend outside longitude/latitude bounds. No wrapping, clamping, implicit transform or spherical strategy is applied. EPSG:3857 uses projected metres; it does not promise a geodesic ground radius of the same size.

Round strategies use 32 points per full circle, with straight sides and symmetric signed distance. The circular chord deviation is `abs(distance) * (1 - cos(pi / 32))`, about 0.004815 times the radius. Boost.Geometry 1.80 also pre-simplifies nonzero-distance input at `abs(distance) / 1000`. Round joins can use an additional subdivision; output vertex count and WKB ordering are not compatibility guarantees. These approximation parameters are not a universal topology or Hausdorff bound for arbitrary complex inputs. Distances and coordinates use floating-point arithmetic. If local-coordinate translation or rounding to WKB doubles loses input precision or output topology, the function returns an error. Zero polygon distance bypasses both buffering and pre-simplification.

## Resource limits

An input is limited to 256 KiB WKB and 5,000 coordinate positions (including ring closure). Output is limited to 10,000 positions. Query memory limits apply to prepared models and temporary allocations. Cancellation and memory errors are checked between rows and around library calls; a single Boost call cannot be interrupted internally. These limits do not guarantee a fixed peak-memory amount or execution time.

## Examples

```SQL
SELECT ROUND(ST_Area(ST_Buffer(
    ST_GeomFromText('POINT (0 0)', 'EPSG:3857'), 1.0)), 6);
-- 3.121445

SELECT ST_Area(ST_Buffer(
    ST_GeomFromText('POLYGON ((0 0,10 0,10 10,0 10,0 0))', 'EPSG:3857'), -1.0));
-- 64

SELECT ST_AsText(ST_Buffer(
    ST_GeomFromText('POLYGON ((0 0,10 0,10 10,0 10,0 0))', 'EPSG:3857'), -5.0));
-- POLYGON EMPTY

SELECT ST_AsText(ST_Buffer(ST_GeomFromText('POINT (0 0)', 'EPSG:4326'), 0));
-- POLYGON EMPTY

SELECT ST_AsText(ST_Buffer(ST_GeomFromText('POINT (0 0)', 'EPSG:3857'), NULL));
-- NULL
```
