---
displayed_sidebar: docs
description: "判断点是否位于线或多线的指定闭区间距离内。"
---

# ST_DWITHIN

判断 `POINT` 与 `LINESTRING` 或 `MULTILINESTRING` 之间的最小距离是否小于或等于阈值。两个几何参数的顺序不限。

对于 `GEOGRAPHY`，函数按球面 OGC:CRS84 边计算，阈值单位为米。对于 `GEOMETRY`，函数按平面边计算，阈值使用声明 CRS 的单位。`GEOMETRY` 输入必须具有兼容的描述符。本函数不执行 CRS 转换。

## 语法

```SQL
BOOLEAN ST_DWITHIN(GEOGRAPHY point_or_line, GEOGRAPHY line_or_point, DOUBLE distance)
BOOLEAN ST_DWITHIN(GEOMETRY point_or_line, GEOMETRY line_or_point, DOUBLE distance)
```

## 参数

- 几何参数必须构成 `POINT`/`LINESTRING` 或 `POINT`/`MULTILINESTRING` 组合。
- `distance` 必须是有限的非负数，并包含相等边界。
- 所有几何值必须是二维的。

`GEOGRAPHY` 线中的精确对跖线段或数值上无法确定的近对跖线段会被拒绝，因为它不能确定唯一的最短球面边。

## 返回值说明

如果任一几何参数或阈值为 `NULL`，则返回 `NULL`。如果任一几何参数为 EMPTY，则返回 `false`。不支持的类型或描述符、无效阈值、混用 `GEOGRAPHY` 和 `GEOMETRY`，以及不兼容的 `GEOMETRY` 描述符都会产生错误。

## 示例

```SQL
SELECT ST_DWITHIN(
    ST_GeogFromText('POINT (0 1)'),
    ST_GeogFromText('LINESTRING (-1 0, 1 0)'),
    111196);
-- true

SELECT ST_DWITHIN(
    ST_GeomFromText('POINT (5 3)', 'EPSG:3857'),
    ST_GeomFromText('LINESTRING (0 0, 10 0)', 'EPSG:3857'),
    3);
-- true
```

## 关键字

ST_DWITHIN,ST,DWITHIN,GEOGRAPHY,GEOMETRY
