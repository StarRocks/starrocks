# ST_Polygon,ST_PolyFromText,ST_PolygonFromText

## Description

Converts a WKT (Well Known Text) to a corresponding polygon memory form.

## Syntax

```Haskell
GEOMETRY ST_Polygon(VARCHAR wkt)
```

## Examples

```Plain Text
MySQL > SELECT ST_AsText(ST_Polygon("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
+------------------------------------------------------------------+
| st_astext(st_polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')) |
+------------------------------------------------------------------+
| POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))                          |
+------------------------------------------------------------------+
```

## keyword

ST_POLYGON,ST_POLYFROMTEXT,ST_POLYGONFROMTEXT,ST,POLYGON,POLYFROMTEXT,POLYGONFROMTEXT
