---
displayed_sidebar: docs
---

# ST_Circle

## Description

Converts a WKT (WEll Known Text) to a circle on the sphere of the earth.

## Syntax

```Haskell
GEOMETRY ST_Circle(DOUBLE center_lng, DOUBLE center_lat, DOUBLE radius)
```

## Parameters

`center_lng` indicates the longitude of the center of the circle.

`center_lat` indicates the latitude of the center of the circle.

`radius` indicates the radius of a circle, in meters. A maximum of 9999999 radius is supported.

## Examples

```Plain Text
MySQL > SELECT ST_AsText(ST_Circle(111, 64, 10000));
+--------------------------------------------+
| st_astext(st_circle(111.0, 64.0, 10000.0)) |
+--------------------------------------------+
| CIRCLE ((111 64), 10000)                   |
+--------------------------------------------+
```

## keyword

ST_CIRCLE,ST,CIRCLE
