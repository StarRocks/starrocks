---
displayed_sidebar: "Chinese"
---

# ST_Polygon,ST_PolyFromText,ST_PolygonFromText

## description

### Syntax

```Haskell
GEOMETRY ST_Polygon(VARCHAR wkt)
```

将一个WKT（Well Known Text）转化为对应的多边形内存形式

## example

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
