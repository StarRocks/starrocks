# ST_Polygon, ST_PolyFromText, ST_PolygonFromText

## 功能

将一个 WKT（Well Known Text）转化为对应的多边形内存形式。

## 语法

```Haskell
ST_Polygon(wkt)
```

## 参数说明

`wkt`: 待转化的 WKT，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 GEOMETRY。

## 示例

```Plain Text
MySQL > SELECT ST_AsText(ST_Polygon("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
+------------------------------------------------------------------+
| st_astext(st_polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')) |
+------------------------------------------------------------------+
| POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))                          |
+------------------------------------------------------------------+
```
