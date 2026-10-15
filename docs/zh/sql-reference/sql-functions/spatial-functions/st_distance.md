---
displayed_sidebar: docs
description: "返回受支持的 GEOGRAPHY 或 GEOMETRY 值之间的最小距离。"
---

# ST_DISTANCE

返回两个受支持值之间的最小距离。点到线的距离取该点到任意线段上最近点的距离，包括线段内部和端点。

对于 `GEOGRAPHY`，函数按球面 OGC:CRS84 边计算，结果单位为米。对于 `GEOMETRY`，函数按平面边计算，结果使用声明 CRS 的单位。`GEOMETRY` 输入必须具有兼容的描述符。本函数不执行 CRS 转换。

## 语法

```SQL
DOUBLE ST_DISTANCE(GEOGRAPHY lhs, GEOGRAPHY rhs)
DOUBLE ST_DISTANCE(GEOMETRY lhs, GEOMETRY rhs)
```

## 参数

支持以下类型组合：

- `POINT` 与 `POINT`
- `POINT` 与 `LINESTRING` 或 `MULTILINESTRING`，参数顺序不限

所有值必须是二维的。`GEOGRAPHY` 线中的精确对跖线段或数值上无法确定的近对跖线段会被拒绝，因为它不能确定唯一的最短球面边。

## 返回值说明

如果任一输入为 `NULL` 或 EMPTY，则返回 `NULL`。不支持的类型或描述符、混用 `GEOGRAPHY` 和 `GEOMETRY`，以及不兼容的 `GEOMETRY` 描述符都会产生错误。

## 示例

```SQL
SELECT ST_DISTANCE(
    ST_GeogFromText('POINT (0 1)'),
    ST_GeogFromText('LINESTRING (-1 0, 1 0)'));
-- 约 111195.1 米

SELECT ST_DISTANCE(
    ST_GeomFromText('POINT (5 3)', 'EPSG:3857'),
    ST_GeomFromText('LINESTRING (0 0, 10 0)', 'EPSG:3857'));
-- 3
```

## 关键字

ST_DISTANCE,ST,DISTANCE,GEOGRAPHY,GEOMETRY
