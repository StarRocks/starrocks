---
displayed_sidebar: docs
description: "使用显式 CRS 从 WKT 构造原生 GEOMETRY，或使用旧版单参数格式。"
---

# ST_GeomFromText, ST_GeometryFromText



`ST_GeomFromText(wkt, crs)` 从二维 OGC Well-Known Text（WKT）构造原生平面 `GEOMETRY` 值。必须显式指定 CRS，该 CRS 会成为原生类型描述符的一部分。

现有的单参数 `ST_GeomFromText` 和 `ST_GeometryFromText` 签名保持不变，继续返回旧版 `VARCHAR` 表示。

## 语法

```Haskell
GEOMETRY ST_GeomFromText(VARCHAR wkt, VARCHAR crs)
VARCHAR ST_GeomFromText(VARCHAR wkt)
VARCHAR ST_GeometryFromText(VARCHAR wkt)
```

## 参数说明

对于原生签名，`crs` 必须是非空字符串字面量。没有默认 CRS，也不会执行隐式坐标转换或重投影。支持全部七种二维 OGC 几何类型、`EMPTY` 和空子元素。无效 WKT 返回 `NULL`；WKT 为 `NULL` 时返回 `NULL`。

## 返回值说明

双参数签名返回原生 `GEOMETRY`。单参数签名返回旧版 `VARCHAR` 表示。

## 示例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('LINESTRING (1 1, 2 2)', 'EPSG:3857'));
+----------------------------------------------------------------------------+
| st_astext(st_geomfromtext('LINESTRING (1 1, 2 2)', 'EPSG:3857'))           |
+----------------------------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                                      |
+----------------------------------------------------------------------------+
```
