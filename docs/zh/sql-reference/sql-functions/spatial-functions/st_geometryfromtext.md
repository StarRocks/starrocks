---
displayed_sidebar: docs
---

# ST_GeometryFromText, ST_GeomFromText

## 功能

将一个 WKT（Well Known Text）转化为对应的内存的几何形式。

## 语法

```Haskell
ST_GeometryFromText(wkt)
```

## 参数说明

`wkt`: 待转化的 WKT，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 GEOMETRY。

## 示例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeometryFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                   |
+---------------------------------------------------------+
```
