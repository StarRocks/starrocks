---
displayed_sidebar: docs
description: "从一个原生平面多边形或多多边形中减去另一个。"
---

# ST_Difference

返回第一个输入中位于第二个输入之外的区域。参数顺序会影响结果。保留孔洞及不相连的组成部分。

## 语法

```SQL
ST_Difference(GEOMETRY lhs, GEOMETRY rhs)
```

## 行为与限制

两个参数必须是具有兼容 CRS 描述符的原生 XY `POLYGON` 或 `MULTIPOLYGON`。支持这两种类型的全部四种组合。返回原生 `GEOMETRY`，保留 CRS 和 XY 坐标：单个面积组成部分为 `POLYGON`，多个组成部分为 `MULTIPOLYGON`，空结果为 `POLYGON EMPTY`。

`NULL` 参数返回 `NULL`。`EMPTY` 表示空区域，不等于 `NULL`。在处理空输入之前，检查非 NULL 输入的结构及拓扑有效性，包括孔洞和多多边形各部分之间的关系。无效输入产生错误，不进行修复。

即使 CRS 为 EPSG:4326，也使用笛卡尔运算（以度为单位的直线边）。EPSG:3857 使用以米为单位的投影坐标。不会隐式转换坐标。不支持 `GEOGRAPHY`、点、线、集合、Z/M、兼容性不符的 CRS、聚合或数组形式及精度网格参数。

计算采用浮点运算，不进行吸附或整数重缩放。WKB 输出为 double 坐标。环方向、起点和组成部分顺序可能改变。如果将输入平移到共同的局部原点或对输出舍入导致拓扑丢失，函数返回错误，不修复或丢弃几何对象。

每个输入最多为 256 KiB WKB 和 5,000 个坐标（包括闭环坐标）。坐标数为 `n` 和 `m` 时，还必须满足 `n*n + m*m + n*m <= 25,000,000`。输出及对称差集的各中间结果最多为 5,000 个坐标；中间结果对也必须满足相同的工作量限制。超出限制返回错误，不截断结果。输入模型及临时分配受查询内存限制约束。在行之间及 overlay 调用前后检查取消状态；单次 Boost 调用没有取消回调。这些限制不保证固定的内存峰值或运行时间。

## 示例

```SQL
SELECT ST_Area(ST_Difference(
    ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))', 'EPSG:3857'),
    ST_GeomFromText('POLYGON ((2 0, 6 0, 6 4, 2 4, 2 0))', 'EPSG:3857')));
-- 8

SELECT ST_AsText(ST_Difference(
    ST_GeomFromText('POLYGON EMPTY', 'EPSG:3857'),
    ST_GeomFromText('POLYGON EMPTY', 'EPSG:3857')));
-- POLYGON EMPTY
```
