---
displayed_sidebar: docs
description: "将 GEOMETRY 或 GEOGRAPHY 值转换为 WKT（Well-Known Text）格式。"
---

# ST_AsText, ST_AsWKT



将 `GEOMETRY` 或 `GEOGRAPHY` 值转换为 WKT（Well-Known Text）格式。`ST_AsWKT` 是 `ST_AsText` 的别名。

## 语法

```Haskell
VARCHAR ST_AsText(GEOMETRY geo)
VARCHAR ST_AsText(GEOGRAPHY geography)
VARCHAR ST_AsWKT(GEOMETRY geo)
VARCHAR ST_AsWKT(GEOGRAPHY geography)
```

## 参数说明

`geo` 或 `geography`：待转换的 `GEOMETRY` 或 `GEOGRAPHY` 值。

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

对于 `GEOGRAPHY`，该函数保留全部七种 OGC 几何类型和 `EMPTY` 成员。输入为 `NULL` 时返回 `NULL`。

```SQL
SELECT ST_AsText(ST_GeogFromText('GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (1 2, 3 4))'));
```

```text
GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (1 2, 3 4))
```
