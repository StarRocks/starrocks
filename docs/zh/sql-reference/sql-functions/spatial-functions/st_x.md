---
displayed_sidebar: docs
description: "返回 GEOGRAPHY 或 GEOMETRY 点的 X 坐标。"
---

# ST_X

返回点的 X 坐标。

对于 `GEOGRAPHY` 值，在 OGC:CRS84 球面语义下，X 表示以度为单位的经度。对于 `GEOMETRY` 值，X 使用其声明的 CRS 单位，并不一定表示经度。输入值必须是非空的 `POINT`。

## 语法

```SQL
DOUBLE ST_X(VARCHAR point)
DOUBLE ST_X(GEOGRAPHY point)
DOUBLE ST_X(GEOMETRY point)
```

## 返回值说明

输入为 `NULL` 时返回 `NULL`。对于原生 `GEOGRAPHY` 和 `GEOMETRY`，如果输入为空值或不是 `POINT` 类型，则返回错误。原生计算支持具有有效描述符的 XY 值。

## 示例

```SQL
SELECT ST_X(ST_Point(24.7, 56.7));
-- 24.7

SELECT ST_X(ST_GeogFromText('POINT (24.7 56.7)'));
-- 24.7

SELECT ST_X(ST_GeomFromText('POINT (1000000.5 2000000)', 'EPSG:3857'));
-- 1000000.5
```
