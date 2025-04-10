---
displayed_sidebar: docs
---

# ST_AsText, ST_AsWKT

## 功能

将一个几何图形转化为 WKT（Well Known Text）的表示形式。

## 语法

```Haskell
ST_AsText(geo)
```

## 参数说明

`geo`: 待转化的参数，支持的数据类型为 GEOMETRY。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
MySQL > SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```
