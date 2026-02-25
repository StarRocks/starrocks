---
displayed_sidebar: docs
---

# ST_Polygon,ST_PolyFromText,ST_PolygonFromText

WKT (Well Known Text) を対応するポリゴンのメモリ形式に変換します。

## 構文

```Haskell
GEOMETRY ST_Polygon(VARCHAR wkt)
```

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_Polygon("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
+------------------------------------------------------------------+
| st_astext(st_polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')) |
+------------------------------------------------------------------+
| POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))                          |
+------------------------------------------------------------------+
```

## キーワード

ST_POLYGON,ST_POLYFROMTEXT,ST_POLYGONFROMTEXT,ST,POLYGON,POLYFROMTEXT,POLYGONFROMTEXT