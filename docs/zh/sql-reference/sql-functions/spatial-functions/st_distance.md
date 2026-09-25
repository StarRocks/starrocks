---
displayed_sidebar: docs
description: "返回两个兼容的 GEOGRAPHY 或 GEOMETRY 点之间的距离。"
---

# ST_DISTANCE

对于 `GEOGRAPHY`，在 OGC:CRS84 语义下返回两个点之间以米为单位的球面距离。

对于 `GEOMETRY`，返回使用声明 CRS 单位的平面欧氏距离。两个输入必须具有兼容的描述符。角度 CRS 中的坐标仍按平面坐标值处理；此重载不会执行球面计算或 CRS 转换。

## 语法

```SQL
DOUBLE ST_DISTANCE(GEOGRAPHY lhs, GEOGRAPHY rhs)
DOUBLE ST_DISTANCE(GEOMETRY lhs, GEOMETRY rhs)
```

## 返回值说明

两个输入值都必须是 `POINT`。如果任一输入为 `NULL` 或 EMPTY，则返回 `NULL`。输入不是 `POINT`、维度或描述符不受支持、混用 `GEOGRAPHY` 和 `GEOMETRY`，或 `GEOMETRY` 描述符不兼容时返回错误。不支持其他几何类型组合。

## 示例

```SQL
SELECT ST_DISTANCE(
    ST_GeogFromText('POINT (0 0)'),
    ST_GeogFromText('POINT (1 0)'));
-- 约 111195.1 米

SELECT ST_DISTANCE(
    ST_GeomFromText('POINT (0 0)', 'EPSG:3857'),
    ST_GeomFromText('POINT (3 4)', 'EPSG:3857'));
-- 5
```

## 关键字

ST_DISTANCE,ST,DISTANCE,GEOGRAPHY,GEOMETRY
