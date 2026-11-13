---
displayed_sidebar: docs
<<<<<<< HEAD
description: "将 WKT（Well Known Text）转换为对应的内存几何形式。"
=======
description: "使用显式 CRS 从 WKT 构造原生 GEOMETRY，或使用旧版单参数格式。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ST_GeometryFromText, ST_GeomFromText



将一个 WKT（Well Known Text）转化为对应的内存的几何形式。

## 语法

```plaintext
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
