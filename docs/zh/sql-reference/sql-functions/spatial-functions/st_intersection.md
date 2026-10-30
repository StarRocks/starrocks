---
displayed_sidebar: docs
description: "返回两个原生平面多边形共有的区域、边和孤立接触点。"
---

# ST_Intersection

返回两个多边形或多多边形共有的集合，包括边界接触。

## 语法

```SQL
ST_Intersection(GEOMETRY lhs, GEOMETRY rhs)
```

## 行为与限制

两个参数必须是具有兼容 CRS 描述符的原生 XY `POLYGON` 或 `MULTIPOLYGON`。支持四种多边形/多多边形输入组合。结果为原生 `GEOMETRY`，保留相同 CRS 和 XY 坐标。

区域返回 `POLYGON` 或 `MULTIPOLYGON`；共有边返回 `LINESTRING` 或 `MULTILINESTRING`；孤立接触点返回 `POINT` 或 `MULTIPOINT`。包含多种维度的结果返回 `GEOMETRYCOLLECTION`。已由区域覆盖的边和点、以及已由返回线覆盖的点不会重复作为单独部分返回。空交集返回 `POLYGON EMPTY`。

`NULL` 参数返回 `NULL`。在处理空输入前检查非 NULL 参数的结构和拓扑有效性。无效输入返回错误，不会自动修复。不支持 `GEOGRAPHY`、点/线/集合输入、Z/M 坐标、不兼容的 CRS 描述符或精度网格参数。

即使使用 EPSG:4326，也执行笛卡尔运算（以度为单位的直线边）；EPSG:3857 使用米。坐标不会隐式转换。输入准备与精度遵循[多边形叠加契约](st_union.md)：使用局部浮点坐标，不进行整数缩放或吸附，WKB 使用 double 坐标。在输入平移和输出舍入后检查多边形拓扑。线段折叠、输出非有限值或不同孤立点舍入为同一个 WKB 点时返回错误。环方向、起点、组成部分顺序及线的分段可以变化。

每个输入最多 256 KiB WKB、5,000 个坐标（包含闭环坐标）。输入计数为 `n` 和 `m` 时，`n*n + m*m + n*m` 不得超过 25,000,000。所有维度的输出坐标总数最多 5,000。超限返回错误，不会截断。查询内存限制适用于准备和临时分配。在行之间以及区域、边界求交和裁剪调用前后检查取消；单次 Boost 调用没有取消回调。这些限制不是固定峰值内存或执行时间保证。

## 示例

```SQL
SELECT ST_Area(ST_Intersection(
    ST_GeomFromText('POLYGON ((0 0,4 0,4 4,0 4,0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((2 0,6 0,6 4,2 4,2 0))', 'EPSG:3857')));
-- 8

SELECT ST_GeometryType(ST_Intersection(
    ST_GeomFromText('POLYGON ((0 0,4 0,4 4,0 4,0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((4 0,8 0,8 4,4 4,4 0))', 'EPSG:3857')));
-- ST_LineString
```
