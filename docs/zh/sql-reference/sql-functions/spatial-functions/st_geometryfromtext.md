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

在查询中，兼容的原生地理空间值可以用作 `ARRAY` 元素、`MAP` 值或 `STRUCT` 字段。您可以构造这些容器、读取元素和字段、使用 `cardinality` 或 `array_length`、用 `UNNEST` 展开数组，也可以使用 `array_map`、`array_filter`，以及不带排序和 `DISTINCT` 的 `array_agg` 和 `map_agg`。

`MAP` 的键不能包含地理空间值。需要对地理空间值进行相等性判断、排序或哈希计算的操作会被拒绝，例如比较、排序和去重。视图、物化视图、SQL UDF、生成列、SQL 执行计划基线和 Pipe 等持久化 SQL 定义不能使用嵌套地理空间表达式。

向客户端返回容器前，请先用 `ST_AsText` 或 `ST_AsBinary` 转换其中的地理空间元素或字段。标量 `GEOGRAPHY` 可以直接返回。标量 `GEOMETRY` 也需要显式转换。

```sql
SELECT array_map(x -> ST_AsText(x), [ST_GeomFromText('POINT (1 2)', 'EPSG:3857')]);
```

## 示例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('LINESTRING (1 1, 2 2)', 'EPSG:3857'));
+----------------------------------------------------------------------------+
| st_astext(st_geomfromtext('LINESTRING (1 1, 2 2)', 'EPSG:3857'))           |
+----------------------------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                                      |
+----------------------------------------------------------------------------+
```
