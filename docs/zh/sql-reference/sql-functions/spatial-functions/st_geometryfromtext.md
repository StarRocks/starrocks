---
displayed_sidebar: docs
description: "将二维 OGC WKT 转换为原生 GEOMETRY 值。"
---

# ST_GeomFromText

将二维 OGC Well-Known Text (WKT) 值转换为原生 `GEOMETRY` 值。

## 语法

```Haskell
GEOMETRY ST_GeomFromText(VARCHAR wkt)
```

为保持兼容性，`ST_GeometryFromText` 继续返回旧版内存 geography 表示。

该函数支持 `POINT`、`LINESTRING`、`POLYGON`、`MULTIPOINT`、`MULTILINESTRING`、`MULTIPOLYGON`、`GEOMETRYCOLLECTION` 及其 `EMPTY` 形式。不支持 EWKT、SRID 和 Z/M 坐标。

## 示例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))'));
+-----------------------------------------------------------------------------------------------+
| st_astext(st_geomfromtext('GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))'))          |
+-----------------------------------------------------------------------------------------------+
| GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))                                       |
+-----------------------------------------------------------------------------------------------+
```

## 关键词

ST_GEOMFROMTEXT,ST,GEOMFROMTEXT
